"""Construcción de las tablas analíticas (etapas KDD: preprocesamiento y transformación).

Motor: Spark (distribuido). Los ~280 mil documentos OCDS anidados (~3 GB de JSON) se
parsean una sola vez y luego se deduplican, desanidan, convierten de moneda y agregan
con Spark SQL sobre Parquet. Es la etapa que más se beneficia del clúster.

Flujo:
    1. Lectura de los .jsonl.gz como texto. Gzip no es divisible: Spark crea una sola
       partición por archivo, así que se reparticiona antes de parsear para que el
       parseo en Python use todos los núcleos.
    2. Una única pasada de Python (`flatten`) produce una fila por registro con los
       hijos (ítems, adjudicaciones, proveedores, contratos) como arrays de structs.
       Se escribe en Parquet (`staging`; sin guion bajo inicial, que Spark ignora como oculto) para no volver a parsear el JSON nunca más.
    3. Deduplicación por ocid (última release_date) y filtro por año de convocatoria.
    4. Las tablas hijas se obtienen con `explode` de las filas ya deduplicadas.
    5. Montos en moneda extranjera sin `amount_PEN` se convierten con medianas de las
       tasas implícitas publicadas por el propio OECE (diaria, anual o del periodo).

Artefactos: data/processed/{processes,items,awards,suppliers,contracts,process_features},
metadata/quality_summary.json, metadata/nulls_by_year.csv, metadata/exchange_rates.csv,
metadata/analytical_schema.json.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType

from src.common import StepTimer, save_csv, save_json
from src.config import PROCESSED, YEARS, spark_session
from src.ingestion.json_records import decode_line
from src.ingestion.validate_files import input_files
from src.processing.model import flatten
from src.processing.text_features import DOMAIN_STOPWORD_DF, SHINGLE_WIDTH, domain_stopwords, shingles_column

BASE = 'ocid source_file release_date effective_date date_source year month'.split()
FIELDS = {
    'processes': BASE + (
        'buyer_id buyer_name buyer_level department category method method_details modality tender_code tender_amount_pen '
        'tender_amount_original currency tender_conversion_method tender_exchange_rate negative_amount extreme_amount '
        'award_amount_pen award_converted_count award_unconverted_count bidders competition amount_band text '
        'normalized_text item_count award_count supplier_count').split(),
    'items': BASE + ('item_id description classification quantity amount_original amount_pen currency '
                     'conversion_method exchange_rate conversion_reference_date').split(),
    'awards': BASE + ('award_id amount_original amount_pen currency conversion_method exchange_rate '
                      'conversion_reference_date supplier_count').split(),
    'suppliers': BASE + ('award_id supplier_id supplier_name buyer_id allocated_amount_pen currency '
                         'conversion_method exchange_rate conversion_reference_date').split(),
    'contracts': BASE + ('contract_id award_id amount_original amount_pen currency conversion_method '
                         'exchange_rate conversion_reference_date status').split(),
}
CHILDREN = ['items', 'awards', 'suppliers', 'contracts']
# Columnas calculadas en Spark después del parseo (no salen de `flatten`).
DERIVED = {'suppliers': ['supplier_name_original', 'supplier_type']}
# Tabla delgada para las Partes II, III y V: lo justo para representar y muestrear procesos.
# Las tablas completas (montos, conversiones, conteos) quedan en `processes`.
FEATURE_COLUMNS = ['ocid', 'effective_date', 'year', 'month', 'buyer_id', 'buyer_level', 'department', 'category',
                   'modality', 'amount_band', 'competition', 'tender_amount_pen', 'text', 'normalized_text', 'shingles']
SUPPLIER_TYPES = ['persona_natural', 'persona_juridica', 'consorcio', 'extranjero', 'otro']
GRAINS = {'items': ['ocid', 'item_id'], 'awards': ['ocid', 'award_id'],
          'suppliers': ['ocid', 'award_id', 'supplier_id'], 'contracts': ['ocid', 'contract_id']}
INTS = set('year month item_count award_count supplier_count award_converted_count award_unconverted_count'.split())
FLOATS = set(('tender_amount_pen tender_amount_original tender_exchange_rate award_amount_pen bidders quantity '
              'amount_original amount_pen allocated_amount_pen exchange_rate').split())
BOOLEANS = {'negative_amount', 'extreme_amount'}


def field_type(name):
    if name in INTS:
        return LongType()
    if name in FLOATS:
        return DoubleType()
    if name in BOOLEANS:
        return BooleanType()
    if name == 'shingles':
        return ArrayType(StringType())
    return StringType()


def output_fields(table):
    return FIELDS[table] + DERIVED.get(table, [])


def schema(table):
    """Esquema final de cada tabla (con las columnas BASE del proceso y las derivadas)."""
    return StructType([StructField(k, field_type(k), True) for k in output_fields(table)])


def child_fields(table):
    """Columnas propias de una tabla hija; las BASE se toman del proceso padre."""
    return [k for k in FIELDS[table] if k not in BASE]


def staging_schema():
    """Una fila por registro: columnas del proceso + un array de structs por tabla hija."""
    children = [StructField(t, ArrayType(StructType([StructField(k, field_type(k), True) for k in child_fields(t)])), True)
                for t in CHILDREN]
    # Solo las columnas que produce `flatten`; las derivadas (DERIVED) se calculan después en Spark.
    process = [StructField(k, field_type(k), True) for k in FIELDS['processes']]
    return StructType(process + children)


def parse(row):
    """Convierte una línea JSONL (uno o más objetos) en filas de staging."""
    rows = []
    for record in decode_line(row.value):
        tables = flatten(record, row.source_file)
        process = tables['processes'][0]
        children = [[tuple(child[k] for k in child_fields(t)) for child in tables[t]] for t in CHILDREN]
        rows.append(tuple(process[k] for k in FIELDS['processes']) + tuple(children))
    return rows


def stage_raw_records(spark):
    """Parsea el JSON una sola vez y lo deja en Parquet."""
    paths = [str(p.resolve()) for p in input_files()]
    partitions = int(spark.conf.get('spark.default.parallelism', '8')) * 2
    raw = (spark.read.text(paths)
           .withColumn('source_file', F.input_file_name())
           .repartition(partitions))
    staged = spark.createDataFrame(raw.rdd.flatMap(parse), staging_schema())
    path = str(PROCESSED / 'staging')
    staged.write.mode('overwrite').parquet(path)
    return spark.read.parquet(path)


def deduplicate(staged):
    """Conserva la versión más reciente de cada ocid (release_date, desempate por archivo)."""
    valid = staged.filter(F.col('ocid').isNotNull() & (F.col('ocid') != ''))
    latest = Window.partitionBy('ocid').orderBy(F.col('release_date').desc(), F.col('source_file').desc())
    return valid.withColumn('_rn', F.row_number().over(latest)).filter('_rn = 1').drop('_rn')


def explode_child(selected, table):
    """Tabla hija a partir de los procesos seleccionados, con las columnas BASE del padre."""
    exploded = selected.select(*BASE, F.explode(table).alias('_child'))
    return (exploded.select(*BASE, *[F.col(f'_child.{k}').alias(k) for k in child_fields(table)])
            .dropDuplicates(GRAINS[table]))


def historical_rates(selected):
    """Medianas de las tasas implícitas (amount_PEN / amount) de convocatorias en moneda extranjera."""
    source = selected.filter("currency != 'PEN' AND tender_exchange_rate > 0")
    median = F.expr('percentile_approx(tender_exchange_rate, 0.5)')
    return dict(
        daily=source.groupBy(F.col('currency').alias('fx_currency'), F.to_date('effective_date').alias('fx_date'))
        .agg(median.alias('daily_rate')),
        annual=source.groupBy(F.col('currency').alias('fx_currency_year'), F.col('year').alias('fx_year'))
        .agg(median.alias('annual_rate')),
        period=source.groupBy(F.col('currency').alias('fx_currency_period')).agg(median.alias('period_rate')),
    )


def apply_historical_rates(df, rates, amount_column='amount_pen', original_column='amount_original'):
    """Completa montos PEN faltantes con la tasa histórica más específica disponible.

    Prioridad: tasa diaria (si está entre 0.5x y 2x la anual, para descartar tasas
    anómalas), luego anual y luego la del periodo. Los montos sin tasa quedan nulos.
    """
    daily, annual, period = rates['daily'], rates['annual'], rates['period']
    reference_date = F.to_date('conversion_reference_date')
    enriched = df.withColumn('_reference_date', reference_date).withColumn('_reference_year', F.year(reference_date))
    enriched = (enriched
                .join(F.broadcast(daily), (enriched.currency == daily.fx_currency) & (enriched._reference_date == daily.fx_date), 'left')
                .join(F.broadcast(annual), (enriched.currency == annual.fx_currency_year) & (enriched._reference_year == annual.fx_year), 'left')
                .join(F.broadcast(period), enriched.currency == period.fx_currency_period, 'left'))
    valid_daily = F.col('daily_rate').isNotNull() & (
        F.col('annual_rate').isNull() | F.col('daily_rate').between(F.col('annual_rate') * .5, F.col('annual_rate') * 2))
    rate = F.coalesce(F.when(valid_daily, F.col('daily_rate')), 'annual_rate', 'period_rate')
    eligible = F.col(amount_column).isNull() & F.col(original_column).isNotNull() & (F.col(original_column) >= 0) & rate.isNotNull()
    method = (F.when(valid_daily, 'oece_daily_median_rate')
              .when(F.col('annual_rate').isNotNull(), 'oece_year_median_rate')
              .otherwise('oece_period_median_rate'))
    # `eligible` se materializa antes de sobrescribir amount_column: si se evaluara después,
    # el monto ya no sería nulo y la tasa y el método no se registrarían.
    return (enriched
            .withColumn('_eligible', eligible).withColumn('_rate', rate).withColumn('_method', method)
            .withColumn(amount_column, F.when(F.col('_eligible'), F.col(original_column) * F.col('_rate')).otherwise(F.col(amount_column)))
            .withColumn('exchange_rate', F.when(F.col('_eligible'), F.col('_rate')).otherwise(F.col('exchange_rate')))
            .withColumn('conversion_method', F.when(F.col('_eligible'), F.col('_method')).otherwise(F.col('conversion_method')))
            .drop('_reference_date', '_reference_year', '_eligible', '_rate', '_method', 'fx_currency', 'fx_date',
                  'daily_rate', 'fx_currency_year', 'fx_year', 'annual_rate', 'fx_currency_period', 'period_rate'))


def allocate_to_suppliers(suppliers, awards):
    """Reparte el monto (ya convertido) de cada adjudicación en partes iguales entre sus proveedores."""
    award_values = awards.select('ocid', 'award_id', F.col('amount_pen').alias('_award_amount_pen'),
                                 F.col('currency').alias('_currency'), F.col('conversion_method').alias('_method'),
                                 F.col('exchange_rate').alias('_rate'), F.col('conversion_reference_date').alias('_date'))
    per_award = Window.partitionBy('ocid', 'award_id')
    return (suppliers.drop('allocated_amount_pen', 'currency', 'conversion_method', 'exchange_rate', 'conversion_reference_date')
            .join(award_values, ['ocid', 'award_id'], 'left')
            .withColumn('allocated_amount_pen', F.when(F.col('supplier_id').isNotNull(), F.col('_award_amount_pen') / F.count('*').over(per_award)))
            .withColumnRenamed('_currency', 'currency').withColumnRenamed('_method', 'conversion_method')
            .withColumnRenamed('_rate', 'exchange_rate').withColumnRenamed('_date', 'conversion_reference_date')
            .select(*FIELDS['suppliers']))


def normalize_suppliers(suppliers):
    """Nombre canónico por RUC y tipo de proveedor.

    - Un mismo RUC aparece con variantes de nombre (tildes, cambios de razón social);
      se usa el nombre de la publicación más reciente y se conserva el original.
    - Tipo según el RUC peruano: 10/15/17 persona natural, 20 persona jurídica. Los
      consorcios reciben identificadores propios de 6-7 dígitos (no son RUC) que casi
      nunca se repiten: ocultan a sus empresas integrantes, lo que fragmenta los
      rankings de concentración. Se marcan para analizarlos aparte, por nombre o por
      longitud del identificador (hay consorcios cuyo nombre no dice "CONSORCIO").
    - Los no domiciliados (identificador L... o 99...) son proveedores extranjeros:
      contratos de Gobierno a Gobierno, defensa, seguros y arbitrajes internacionales.
    """
    latest = Window.partitionBy('supplier_id').orderBy(F.col('release_date').desc(), F.col('ocid').desc())
    identifier = F.regexp_extract('supplier_id', r'^PE-RUC-(.+)$', 1)
    digits = F.regexp_extract('supplier_id', r'(\d+)$', 1)
    name = F.upper(F.coalesce('supplier_name', F.lit('')))
    supplier_type = (F.when(identifier.startswith('L') | identifier.startswith('99'), 'extranjero')
                     .when(name.rlike(r'(^|\s)CONSORCIO\b') | F.length(identifier).between(6, 7), 'consorcio')
                     .when((F.length(digits) == 11) & digits.rlike('^(10|15|17)'), 'persona_natural')
                     .when((F.length(digits) == 11) & digits.startswith('20'), 'persona_juridica')
                     .otherwise('otro'))
    return (suppliers
            .withColumn('supplier_name_original', F.col('supplier_name'))
            .withColumn('supplier_name', F.first('supplier_name').over(latest))
            .withColumn('supplier_type', supplier_type)
            .select(*output_fields('suppliers')))


def supplier_report(suppliers):
    """Variantes de nombre por RUC y peso de cada tipo de proveedor en el monto atribuido."""
    variants = (suppliers.groupBy('supplier_id').agg(F.countDistinct('supplier_name_original').alias('names'))
                .filter('names > 1').count())
    rows = (suppliers.groupBy('supplier_type')
            .agg(F.count('*').alias('rows'), F.countDistinct('supplier_id').alias('ids'),
                 F.sum('allocated_amount_pen').alias('allocated_amount_pen')).collect())
    total = sum(r['allocated_amount_pen'] or 0 for r in rows) or 1
    return dict(
        ids_with_name_variants=variants,
        types={r['supplier_type']: dict(rows=r['rows'], ids=r['ids'], amount_share=(r['allocated_amount_pen'] or 0) / total)
               for r in rows},
        policy='Nombre canónico = nombre en la publicación más reciente del RUC. Tipo por prefijo del RUC; '
               'consorcios por nombre (sus identificadores no son RUC y no revelan a los integrantes).')


def recompute_award_totals(processes, awards):
    """El monto adjudicado del proceso se recalcula con las adjudicaciones ya convertidas."""
    stats = awards.groupBy('ocid').agg(
        F.sum('amount_pen').alias('award_amount_pen'),
        F.sum(F.when(F.col('amount_pen').isNotNull() & (F.col('conversion_method') != 'native_pen'), 1).otherwise(0)).alias('_converted'),
        F.sum(F.when(F.col('amount_pen').isNull(), 1).otherwise(0)).alias('_unconverted'))
    return (processes.drop('award_amount_pen', 'award_converted_count', 'award_unconverted_count')
            .join(stats, 'ocid', 'left')
            .withColumn('award_converted_count', F.coalesce('_converted', F.lit(0)))
            .withColumn('award_unconverted_count', F.coalesce('_unconverted', F.lit(0)))
            .select(*FIELDS['processes']))


def is_blank(column):
    return F.col(column).isNull() | (F.col(column).cast('string') == '')


def nulls_by_year(processes):
    """Porcentaje de vacíos por año en los campos que limitan el análisis (una sola agregación)."""
    fields = ['department', 'category', 'buyer_id', 'tender_amount_pen', 'award_amount_pen', 'normalized_text', 'bidders']
    rows = (processes.groupBy('year')
            .agg(F.count('*').alias('n'), *[F.sum(F.when(is_blank(k), 1).otherwise(0)).alias(k) for k in fields])
            .orderBy('year').collect())
    return [dict(year=r['year'], field=k, nulls=r[k], count=r['n'], percentage=100 * r[k] / max(1, r['n']))
            for r in rows for k in fields]


def buyer_level_report(selected):
    """Cobertura de la clasificación por nivel de gobierno (entidades y procesos)."""
    entities = (selected.groupBy('buyer_id', 'buyer_name', 'buyer_level').agg(F.count('*').alias('processes'))
                .orderBy(F.desc('processes')).toPandas())
    by_level = entities.groupby('buyer_level').agg(entities=('buyer_id', 'count'), processes=('processes', 'sum'))
    total = int(by_level.processes.sum())
    summary = {level: dict(entities=int(r.entities), processes=int(r.processes), share=r.processes / total)
               for level, r in by_level.iterrows()}
    return entities, dict(levels=summary, classified_share=1 - summary.get('otro', {}).get('share', 0.0),
                          method='Reglas ordenadas sobre el nombre oficial (src/processing/entity_level.py)')


def quality_report(staged, unique, selected, tables):
    """Resumen de calidad: descartes, duplicados, fechas, monedas y cobertura monetaria."""
    raw = staged.agg(F.count('*').alias('total'), F.sum(F.when(is_blank('ocid'), 1).otherwise(0)).alias('missing')).first()
    dedup = unique.agg(F.count('*').alias('unique'), F.sum(F.when(F.col('effective_date').isNull(), 1).otherwise(0)).alias('no_date')).first()
    s = selected.agg(
        F.count('*').alias('n'),
        F.sum(F.col('tender_amount_pen').isNotNull().cast('int')).alias('tender_with_pen'),
        F.sum((F.col('award_count') == 0).cast('int')).alias('no_award'),
        F.sum(((F.col('award_count') > 0) & F.col('award_amount_pen').isNull()).cast('int')).alias('award_without_amount'),
        F.sum(F.col('negative_amount').cast('int')).alias('negative'),
        F.sum((F.col('currency') != 'PEN').cast('int')).alias('non_pen'),
        F.sum(F.col('extreme_amount').cast('int')).alias('extreme'),
        F.min('effective_date').alias('min_date'), F.max('effective_date').alias('max_date')).first()
    awards = tables['awards']
    by_method = {r['conversion_method']: r.asDict() for r in
                 awards.groupBy('conversion_method').agg(F.count('*').alias('awards'), F.sum('amount_pen').alias('amount_pen')).collect()}
    unconvertible = 'amount_original is not null AND amount_pen is null'
    policy = ('amount_PEN de la fuente; PEN nativo; tasa implícita de la convocatoria si coincide la moneda; después mediana '
              'histórica OECE diaria, anual y del periodo para la moneda original.')
    return dict(
        raw_records=raw['total'], missing_ocid=raw['missing'], duplicate_ocids=raw['total'] - raw['missing'] - dedup['unique'],
        outside_period_or_missing_date=dedup['unique'] - s['n'], missing_date=dedup['no_date'], selected_processes=s['n'],
        table_counts={'processes': s['n'], **{t: tables[t].count() for t in CHILDREN}},
        negative_amounts=s['negative'], non_pen=s['non_pen'], extreme_amounts_over_1bn=s['extreme'],
        awards_without_supplier=awards.filter('supplier_count = 0').count(),
        min_date=s['min_date'], max_date=s['max_date'],
        supplier_allocation='Monto dividido equitativamente entre proveedores de la adjudicación; adjudicaciones sin proveedor permanecen en totales y no entran al ranking.',
        currency_policy=policy,
        missing_amount_policy='Sin adjudicación y monto no convertible permanecen nulos; nunca se interpretan como cero.',
        outlier_policy='Se conserva el monto original y se marca > S/1,000 millones; las vistas técnicas reportan resultados con y sin extremos.',
        categorical_policy='Los faltantes categóricos se agrupan como NO INFORMADO en agregaciones, conservando el nulo original en la capa procesada.',
        date_policy='Convocatoria: tender.datePublished; fallback tenderPeriod.startDate. Sin fallback a fecha de actualización.',
        duplicate_policy='Última release_date por ocid; desempate source_file',
        monetary_coverage=dict(
            tender_processes_with_pen=s['tender_with_pen'], tender_processes_total=s['n'],
            processes_without_awards=s['no_award'], processes_with_awards_without_convertible_amount=s['award_without_amount'],
            awards_by_conversion_method=by_method,
            items_without_convertible_amount=tables['items'].filter(unconvertible).count(),
            contracts_without_convertible_amount=tables['contracts'].filter(unconvertible).count(),
            policy=policy),
    )


def write_partitioned(df, name):
    """Escribe particionado por año y mes, con un archivo por partición.

    Sin el repartition, cada una de las tareas escribe un archivo en cada carpeta
    año/mes que toca (8 tareas x 36 meses = 288 archivos pequeños por tabla). Con
    repartition('year', 'month') cada mes queda en una sola tarea y un solo archivo:
    menos archivos que abrir al leer y una escritura mucho más rápida.
    """
    (df.repartition('year', 'month')
     .write.mode('overwrite').partitionBy('year', 'month').parquet(str(PROCESSED / name)))


def run(spark=None):
    s = spark or spark_session()
    timer = StepTimer()
    with timer.step('lectura, parseo y staging'):
        staged = stage_raw_records(s)
    with timer.step('deduplicación y filtro de años'):
        unique = deduplicate(staged).cache()
        selected_rows = unique.filter(F.col('year').isin(YEARS)).cache()
        if selected_rows.count() == 0:
            raise ValueError('No hay procesos con fecha de convocatoria en los años seleccionados')
    with timer.step('tasas históricas'):
        rates = historical_rates(selected_rows)
        save_csv('metadata/exchange_rates.csv', rates['daily'].orderBy('fx_currency', 'fx_date').toPandas())

    tables = {t: explode_child(selected_rows, t) for t in CHILDREN}
    for name in ['items', 'awards', 'contracts']:
        tables[name] = apply_historical_rates(tables[name], rates)
    tables['suppliers'] = normalize_suppliers(allocate_to_suppliers(tables['suppliers'], tables['awards']))
    with timer.step('stopwords de dominio'):
        stopwords = domain_stopwords(selected_rows)
        save_json('metadata/domain_stopwords.json', dict(
            threshold=DOMAIN_STOPWORD_DF, shingle_width=SHINGLE_WIDTH, documents=selected_rows.count(), terms=stopwords))
    selected = recompute_award_totals(selected_rows.select(*FIELDS['processes']), tables['awards']).cache()
    features = (selected.withColumn('shingles', shingles_column([t['term'] for t in stopwords]))
                .select(*FEATURE_COLUMNS))
    with timer.step('escritura processes'):
        write_partitioned(selected, 'processes')
    for name, df in tables.items():
        with timer.step(f'escritura {name}'):
            write_partitioned(df, name)
    with timer.step('escritura process_features'):
        write_partitioned(features, 'process_features')
    with timer.step('reporte de calidad'):
        # Releer lo escrito evita recalcular conversiones y joins en el reporte de calidad.
        # El esquema es explícito porque una tabla vacía no deja archivos de los que inferirlo.
        written = {t: s.read.schema(schema(t)).parquet(str(PROCESSED / t)) for t in CHILDREN}
        quality = quality_report(staged, unique, selected, written)
        save_csv('metadata/nulls_by_year.csv', nulls_by_year(selected))
        entities, quality['buyer_levels'] = buyer_level_report(selected)
        quality['suppliers'] = supplier_report(written['suppliers'])
        save_csv('metadata/buyer_levels.csv', entities)
    save_json('metadata/quality_summary.json', quality)
    save_json('metadata/analytical_schema.json',
              {**{k: schema(k).jsonValue() for k in FIELDS}, 'process_features': features.schema.jsonValue()})
    save_json('metadata/processing_timings.json', timer.steps)
    for df in (unique, selected_rows, selected):
        df.unpersist()
    return quality

if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
