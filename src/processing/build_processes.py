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
 'processes': BASE + 'buyer_id buyer_name department category method method_details tender_amount_pen tender_amount_original currency negative_amount award_amount_pen award_unconverted_count bidders competition amount_band text normalized_text shingles item_count award_count supplier_count'.split(),
 'items': BASE + 'item_id description classification quantity amount_pen'.split(),
 'awards': BASE + 'award_id amount_pen currency supplier_count'.split(),
 'suppliers': BASE + 'award_id supplier_id supplier_name buyer_id allocated_amount_pen'.split(),
 'contracts': BASE + 'contract_id award_id amount_pen status'.split(),
}

def schema(table):
    ints = set('year month item_count award_count supplier_count award_unconverted_count'.split())
    floats = set('tender_amount_pen tender_amount_original award_amount_pen bidders quantity amount_pen allocated_amount_pen'.split())
    return StructType([StructField(k, LongType() if k in ints else DoubleType() if k in floats else BooleanType() if k == 'negative_amount' else ArrayType(StringType()) if k == 'shingles' else StringType(), True) for k in FIELDS[table]])


def parse(row):
    return [flatten(record, row.source_file) for record in decode_line(row.value)]


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
    counts = {'processes': n}
    for name in FIELDS:
        df = selected if name == 'processes' else s.createDataFrame(parsed.flatMap(lambda r, name=name: r[name]), schema(name)).join(keys, ['ocid','source_file','release_date'], 'inner').dropDuplicates(grains[name])
        counts[name] = df.count()
        df.write.mode('overwrite').partitionBy('year','month').parquet(str(PROCESSED / name))
    selected.write.mode('overwrite').partitionBy('year','month').parquet(str(PROCESSED / 'process_features'))
    nulls = []
    for year in YEARS:
        part = selected.filter(F.col('year') == year)
        agg = part.agg(F.count('*').alias('n'), *[F.sum(F.when(F.col(k).isNull() | (F.col(k).cast('string') == ''), 1).otherwise(0)).alias(k) for k in ['department','category','buyer_id','tender_amount_pen','award_amount_pen','normalized_text','bidders']]).first().asDict()
        for k,v in agg.items():
            if k != 'n': nulls.append(dict(year=year, field=k, nulls=v or 0, count=agg['n'], percentage=100*(v or 0)/max(1,agg['n'])))
    quality = dict(raw_records=total, missing_ocid=missing, duplicate_ocids=total-missing-unique_count, outside_period_or_missing_date=unique_count-n, missing_date=unique.filter('effective_date is null').count(), selected_processes=n, table_counts=counts, negative_amounts=selected.filter('negative_amount').count(), non_pen=selected.filter("currency != 'PEN'").count(), extreme_amounts_over_1bn=selected.filter('tender_amount_pen > 1000000000').count(), awards_without_supplier=s.read.parquet(str(PROCESSED/'awards')).filter('supplier_count = 0').count(), min_date=selected.agg(F.min('effective_date')).first()[0], max_date=selected.agg(F.max('effective_date')).first()[0], supplier_allocation='Monto dividido equitativamente entre proveedores de la adjudicación; no representa pagos individuales', date_policy='Convocatoria: tender.datePublished; fallback tenderPeriod.startDate. Sin fallback a fecha de actualización.', duplicate_policy='Última release_date por ocid; desempate source_file')
    save_json('metadata/quality_summary.json', quality)
    save_json('metadata/analytical_schema.json', {k: schema(k).jsonValue() for k in FIELDS})
    save_csv('metadata/nulls_by_year.csv', nulls)
    parsed.unpersist(); p.unpersist(); unique.unpersist(); selected.unpersist()
    return quality

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
