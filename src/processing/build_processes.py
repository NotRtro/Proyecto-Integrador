import json
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, ArrayType
from src.config import YEARS, PROCESSED, ARTIFACTS, spark_session
from src.common import save_json, save_csv
from src.ingestion.validate_files import input_files
from src.processing.model import flatten
from src.ingestion.json_records import decode_line

BASE = 'ocid source_file release_date effective_date date_source year month'.split()
FIELDS = {
 'processes': BASE + 'buyer_id buyer_name department category method method_details tender_amount_pen tender_amount_original currency tender_conversion_method tender_exchange_rate negative_amount extreme_amount award_amount_pen award_converted_count award_unconverted_count bidders competition amount_band text normalized_text shingles item_count award_count supplier_count'.split(),
 'items': BASE + 'item_id description classification quantity amount_original amount_pen currency conversion_method exchange_rate conversion_reference_date'.split(),
 'awards': BASE + 'award_id amount_original amount_pen currency conversion_method exchange_rate conversion_reference_date supplier_count'.split(),
 'suppliers': BASE + 'award_id supplier_id supplier_name buyer_id allocated_amount_pen currency conversion_method exchange_rate conversion_reference_date'.split(),
 'contracts': BASE + 'contract_id award_id amount_original amount_pen currency conversion_method exchange_rate conversion_reference_date status'.split(),
}

def schema(table):
    ints = set('year month item_count award_count supplier_count award_converted_count award_unconverted_count'.split())
    floats = set('tender_amount_pen tender_amount_original tender_exchange_rate award_amount_pen bidders quantity amount_original amount_pen allocated_amount_pen exchange_rate'.split())
    booleans = {'negative_amount', 'extreme_amount'}
    return StructType([StructField(k, LongType() if k in ints else DoubleType() if k in floats else BooleanType() if k in booleans else ArrayType(StringType()) if k == 'shingles' else StringType(), True) for k in FIELDS[table]])


def parse(row):
    return [flatten(record, row.source_file) for record in decode_line(row.value)]


def apply_historical_rates(df, amount_column='amount_pen', original_column='amount_original'):
    """Fill missing PEN values with OECE-derived historical median rates."""
    reference_date = F.to_date('conversion_reference_date')
    enriched = df.withColumn('_reference_date', reference_date).withColumn('_reference_year', F.year(reference_date))
    daily = apply_historical_rates.daily
    annual = apply_historical_rates.annual
    period = apply_historical_rates.period
    enriched = (enriched
        .join(F.broadcast(daily), (enriched.currency == daily.fx_currency) & (enriched._reference_date == daily.fx_date), 'left')
        .join(F.broadcast(annual), (enriched.currency == annual.fx_currency_year) & (enriched._reference_year == annual.fx_year), 'left')
        .join(F.broadcast(period), enriched.currency == period.fx_currency_period, 'left'))
    valid_daily = F.col('daily_rate').isNotNull() & (F.col('annual_rate').isNull() | F.col('daily_rate').between(F.col('annual_rate') * .5, F.col('annual_rate') * 2))
    rate = F.coalesce(F.when(valid_daily, F.col('daily_rate')), 'annual_rate', 'period_rate')
    eligible = F.col(amount_column).isNull() & F.col(original_column).isNotNull() & (F.col(original_column) >= 0) & rate.isNotNull()
    method = (F.when(valid_daily, 'oece_daily_median_rate')
        .when(F.col('annual_rate').isNotNull(), 'oece_year_median_rate')
        .otherwise('oece_period_median_rate'))
    return (enriched
        .withColumn('_fallback_eligible', eligible)
        .withColumn('_fallback_rate', rate)
        .withColumn('_fallback_method', method)
        .withColumn(amount_column, F.when(F.col('_fallback_eligible'), F.col(original_column) * F.col('_fallback_rate')).otherwise(F.col(amount_column)))
        .withColumn('exchange_rate', F.when(F.col('_fallback_eligible'), F.col('_fallback_rate')).otherwise(F.col('exchange_rate')))
        .withColumn('conversion_method', F.when(F.col('_fallback_eligible'), F.col('_fallback_method')).otherwise(F.col('conversion_method')))
        .drop('_reference_date', '_reference_year', '_fallback_eligible', '_fallback_rate', '_fallback_method', 'fx_currency', 'fx_date', 'daily_rate', 'fx_currency_year', 'fx_year', 'annual_rate', 'fx_currency_period', 'period_rate'))


def run(spark=None):
    s = spark or spark_session()
    raw = s.read.text([str(p.resolve()) for p in input_files()]).withColumn('source_file', F.input_file_name())
    parsed = raw.rdd.flatMap(parse).persist()
    p = s.createDataFrame(parsed.flatMap(lambda r: r['processes']), schema('processes')).cache()
    total = p.count()
    missing = p.filter(F.col('ocid').isNull() | (F.col('ocid') == '')).count()
    w = Window.partitionBy('ocid').orderBy(F.col('release_date').desc(), F.col('source_file').desc())
    unique = p.filter(F.col('ocid').isNotNull() & (F.col('ocid') != '')).withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn').cache()
    unique_count = unique.count()
    selected = unique.filter(F.col('year').isin(YEARS)).cache()
    n = selected.count()
    if n == 0:
        raise ValueError('No hay procesos con fecha de convocatoria en los años seleccionados')
    keys = selected.select('ocid', 'source_file', 'release_date')
    grains = {'items': ['ocid','item_id'], 'awards': ['ocid','award_id'], 'suppliers': ['ocid','award_id','supplier_id'], 'contracts': ['ocid','contract_id']}
    rate_source = selected.filter("currency != 'PEN' AND tender_exchange_rate > 0")
    apply_historical_rates.daily = rate_source.groupBy(F.col('currency').alias('fx_currency'), F.to_date('effective_date').alias('fx_date')).agg(F.expr('percentile_approx(tender_exchange_rate, 0.5)').alias('daily_rate'))
    apply_historical_rates.annual = rate_source.groupBy(F.col('currency').alias('fx_currency_year'), F.col('year').alias('fx_year')).agg(F.expr('percentile_approx(tender_exchange_rate, 0.5)').alias('annual_rate'))
    apply_historical_rates.period = rate_source.groupBy(F.col('currency').alias('fx_currency_period')).agg(F.expr('percentile_approx(tender_exchange_rate, 0.5)').alias('period_rate'))
    save_csv('metadata/exchange_rates.csv', apply_historical_rates.daily.orderBy('fx_currency', 'fx_date').toPandas())

    tables = {}
    for name in grains:
        tables[name] = s.createDataFrame(parsed.flatMap(lambda r, name=name: r[name]), schema(name)).join(keys, ['ocid','source_file','release_date'], 'inner').dropDuplicates(grains[name])
    for name in ['items', 'awards', 'contracts']:
        tables[name] = apply_historical_rates(tables[name])

    award_values = tables['awards'].select('ocid', 'award_id', F.col('amount_pen').alias('_award_amount_pen'), F.col('currency').alias('_award_currency'), F.col('conversion_method').alias('_award_conversion_method'), F.col('exchange_rate').alias('_award_exchange_rate'), F.col('conversion_reference_date').alias('_award_reference_date'))
    supplier_base = tables['suppliers'].drop('allocated_amount_pen', 'currency', 'conversion_method', 'exchange_rate', 'conversion_reference_date')
    tables['suppliers'] = (supplier_base.join(award_values, ['ocid', 'award_id'], 'left')
        .withColumn('allocated_amount_pen', F.when(F.col('supplier_id').isNotNull(), F.col('_award_amount_pen') / F.count('*').over(Window.partitionBy('ocid', 'award_id'))))
        .withColumnRenamed('_award_currency', 'currency')
        .withColumnRenamed('_award_conversion_method', 'conversion_method')
        .withColumnRenamed('_award_exchange_rate', 'exchange_rate')
        .withColumnRenamed('_award_reference_date', 'conversion_reference_date')
        .drop('_award_amount_pen'))

    award_stats = tables['awards'].groupBy('ocid').agg(
        F.sum('amount_pen').alias('_award_amount_pen'),
        F.sum(F.when(F.col('amount_pen').isNotNull() & (F.col('conversion_method') != 'native_pen'), 1).otherwise(0)).alias('_award_converted_count'),
        F.sum(F.when(F.col('amount_pen').isNull(), 1).otherwise(0)).alias('_award_unconverted_count'))
    selected_base = selected
    selected = (selected_base.drop('award_amount_pen', 'award_converted_count', 'award_unconverted_count').join(award_stats, 'ocid', 'left')
        .withColumnRenamed('_award_amount_pen', 'award_amount_pen')
        .withColumn('award_converted_count', F.coalesce(F.col('_award_converted_count'), F.lit(0)))
        .withColumn('award_unconverted_count', F.coalesce(F.col('_award_unconverted_count'), F.lit(0)))
        .drop('_award_converted_count', '_award_unconverted_count').cache())
    selected.count()
    counts = {'processes': n}
    selected.write.mode('overwrite').partitionBy('year','month').parquet(str(PROCESSED / 'processes'))
    for name, df in tables.items():
        counts[name] = df.count()
        df.write.mode('overwrite').partitionBy('year','month').parquet(str(PROCESSED / name))
    selected.write.mode('overwrite').partitionBy('year','month').parquet(str(PROCESSED / 'process_features'))
    nulls = []
    for year in YEARS:
        part = selected.filter(F.col('year') == year)
        agg = part.agg(F.count('*').alias('n'), *[F.sum(F.when(F.col(k).isNull() | (F.col(k).cast('string') == ''), 1).otherwise(0)).alias(k) for k in ['department','category','buyer_id','tender_amount_pen','award_amount_pen','normalized_text','bidders']]).first().asDict()
        for k,v in agg.items():
            if k != 'n': nulls.append(dict(year=year, field=k, nulls=v or 0, count=agg['n'], percentage=100*(v or 0)/max(1,agg['n'])))
    awards_df = tables['awards']
    conversion_rows = {r['conversion_method']: r.asDict() for r in awards_df.groupBy('conversion_method').agg(F.count('*').alias('awards'), F.sum('amount_pen').alias('amount_pen')).collect()}
    no_award = selected.filter('award_count = 0').count()
    missing_award = selected.filter('award_count > 0 AND award_amount_pen is null').count()
    monetary_coverage = dict(
        tender_processes_with_pen=selected.filter('tender_amount_pen is not null').count(),
        tender_processes_total=n,
        processes_without_awards=no_award,
        processes_with_awards_without_convertible_amount=missing_award,
        awards_by_conversion_method=conversion_rows,
        items_without_convertible_amount=tables['items'].filter('amount_original is not null AND amount_pen is null').count(),
        contracts_without_convertible_amount=tables['contracts'].filter('amount_original is not null AND amount_pen is null').count(),
        policy='amount_PEN de la fuente; PEN nativo; tasa implícita de la convocatoria si coincide la moneda; después mediana histórica OECE diaria, anual y del periodo para la moneda original.',
    )
    quality = dict(raw_records=total, missing_ocid=missing, duplicate_ocids=total-missing-unique_count, outside_period_or_missing_date=unique_count-n, missing_date=unique.filter('effective_date is null').count(), selected_processes=n, table_counts=counts, negative_amounts=selected.filter('negative_amount').count(), non_pen=selected.filter("currency != 'PEN'").count(), extreme_amounts_over_1bn=selected.filter('extreme_amount').count(), awards_without_supplier=awards_df.filter('supplier_count = 0').count(), min_date=selected.agg(F.min('effective_date')).first()[0], max_date=selected.agg(F.max('effective_date')).first()[0], supplier_allocation='Monto dividido equitativamente entre proveedores de la adjudicación; adjudicaciones sin proveedor permanecen en totales y no entran al ranking.', currency_policy=monetary_coverage['policy'], missing_amount_policy='Sin adjudicación y monto no convertible permanecen nulos; nunca se interpretan como cero.', outlier_policy='Se conserva el monto original y se marca > S/1,000 millones; las vistas técnicas reportan resultados con y sin extremos.', categorical_policy='Los faltantes categóricos se agrupan como NO INFORMADO en agregaciones, conservando el nulo original en la capa procesada.', date_policy='Convocatoria: tender.datePublished; fallback tenderPeriod.startDate. Sin fallback a fecha de actualización.', duplicate_policy='Última release_date por ocid; desempate source_file', monetary_coverage=monetary_coverage)
    save_json('metadata/quality_summary.json', quality)
    save_json('metadata/analytical_schema.json', {k: schema(k).jsonValue() for k in FIELDS})
    save_csv('metadata/nulls_by_year.csv', nulls)
    parsed.unpersist(); p.unpersist(); unique.unpersist(); selected_base.unpersist(); selected.unpersist()
    return quality

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
