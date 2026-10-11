"""Análisis exploratorio con Spark DataFrames y Spark SQL (Parte I).

Motor: Spark. Cada función devuelve un DataFrame agregado y pequeño; `run.py` lo
exporta como artefacto para la web (calcular una vez, explorar muchas).

Cobertura del EDA exigido por el enunciado:
- Distribución temporal: monthly_evolution (año, mes) y totales anuales en Spark SQL.
- Variables categóricas: categoría, modalidad peruana (modality, agrupada desde
  procurementMethodDetails), departamento, nivel de gobierno,
  competencia.
- Montos: histograma en escala logarítmica y percentiles por año y categoría (los
  montos públicos tienen colas muy largas; la media sola engaña).
- Frecuencia de entidades y proveedores: rankings en concentration.py.
- Pregunta Q2: tasa de procesos con un solo postor por cada dimensión.
"""
from pyspark.sql import functions as F

NOT_INFORMED = 'NO INFORMADO'
CATEGORICAL = ['department', 'category', 'buyer_id', 'buyer_name', 'buyer_level', 'method', 'method_details', 'modality']
CUBE_DIMENSIONS = ['year', 'month', 'department', 'category', 'modality', 'buyer_level', 'buyer_id', 'buyer_name',
                   'competition', 'amount_band']
def amounts():
    """Medidas comunes de cada agregación (función: una Column necesita SparkSession activa)."""
    return [F.count('*').alias('processes'), F.sum('tender_amount_pen').alias('tender_amount_pen'),
            F.sum('award_amount_pen').alias('award_amount_pen')]



PERCENTILES = [0.1, 0.25, 0.5, 0.75, 0.9, 0.99]
LOG_BIN = 0.25  # ancho del intervalo en log10(soles): cuatro intervalos por orden de magnitud
SINGLE_BIDDER_DIMENSIONS = ['modality', 'category', 'department', 'amount_band', 'buyer_level']


def with_informed_categories(processes):
    """Los faltantes categóricos se agrupan como NO INFORMADO (el nulo original queda en Parquet)."""
    for column in CATEGORICAL:
        processes = processes.withColumn(column, F.coalesce(F.col(column), F.lit(NOT_INFORMED)))
    return processes


def filter_cube(processes):
    """Cubo para los filtros de la web: cada combinación de dimensiones con sus totales."""
    return processes.groupBy(*CUBE_DIMENSIONS).agg(*amounts())


def distributions(processes):
    """Distribuciones por año de cada dimensión categórica y temporal."""
    return {
        'monthly_evolution': processes.groupBy('year', 'month').agg(*amounts()).orderBy('year', 'month'),
        'category_distribution': processes.groupBy('year', 'category').agg(*amounts()),
        'modality_distribution': processes.groupBy('year', 'modality', 'category').agg(*amounts()),
        'department_distribution': processes.groupBy('year', 'department').agg(*amounts()),
        'buyer_level_distribution': processes.groupBy('year', 'buyer_level').agg(*amounts()),
        'competition_distribution': processes.groupBy('year', 'competition').agg(*amounts()),
        'amount_distribution': processes.groupBy('year', 'amount_band').count(),
    }


def yearly_totals_sql(spark, processes):
    """La misma agregación anual escrita en Spark SQL (la web la usa como serie de referencia)."""
    processes.createOrReplaceTempView('processes')
    return spark.sql("""
        SELECT year,
               COUNT(*)                AS processes,
               SUM(tender_amount_pen)  AS tender_amount_pen,
               SUM(award_amount_pen)   AS award_amount_pen
        FROM processes
        GROUP BY year
        ORDER BY year""")


def amount_histogram(processes):
    """Histograma de log10(monto convocado) por año y categoría."""
    positive = processes.filter(F.col('tender_amount_pen') > 0)
    bin_start = F.floor(F.log10('tender_amount_pen') / LOG_BIN) * LOG_BIN
    return (positive.withColumn('log10_bin', bin_start)
            .groupBy('year', 'category', 'log10_bin').agg(F.count('*').alias('processes'),
                                                          F.sum('tender_amount_pen').alias('tender_amount_pen'))
            .withColumn('bin_from_pen', F.pow(F.lit(10.0), 'log10_bin'))
            .withColumn('bin_to_pen', F.pow(F.lit(10.0), F.col('log10_bin') + LOG_BIN))
            .orderBy('year', 'category', 'log10_bin'))


def amount_percentiles(processes):
    """Percentiles del monto convocado por año y categoría (percentile_approx, distribuido)."""
    quantiles = F.expr(f"percentile_approx(tender_amount_pen, array({', '.join(map(str, PERCENTILES))}), 10000)")
    grouped = (processes.filter(F.col('tender_amount_pen').isNotNull())
               .groupBy('year', 'category')
               .agg(F.count('*').alias('processes'), F.avg('tender_amount_pen').alias('mean'), quantiles.alias('q')))
    for index, p in enumerate(PERCENTILES):
        grouped = grouped.withColumn(f'p{int(p * 100)}', F.col('q')[index])
    return grouped.drop('q').orderBy('year', 'category')


def amount_outliers(processes):
    """Sensibilidad de los totales a montos extremos (> S/ 1 000 millones)."""
    return processes.groupBy('year').agg(
        F.count('*').alias('processes'),
        F.sum(F.when(F.col('extreme_amount'), 1).otherwise(0)).alias('extreme_processes'),
        F.sum('tender_amount_pen').alias('tender_amount_all'),
        F.sum(F.when(~F.col('extreme_amount'), F.col('tender_amount_pen'))).alias('tender_amount_without_extremes'),
        F.expr('percentile_approx(tender_amount_pen, 0.5)').alias('median_tender_amount'),
        F.max('tender_amount_pen').alias('max_tender_amount'))


def single_bidder_rates(processes):
    """Q2: proporción de procesos con un solo postor por dimensión, global y por año.

    La tasa se calcula solo sobre procesos con número de postores informado; la
    proporción sin dato se reporta aparte porque, si es alta, la tasa no es
    representativa de esa dimensión.
    """
    known = F.col('competition') != 'sin dato'
    frames = []
    for dimension in SINGLE_BIDDER_DIMENSIONS:
        for by_year in (False, True):
            keys = [dimension] + (['year'] if by_year else [])
            frame = (processes.groupBy(*keys).agg(
                F.count('*').alias('processes'),
                F.sum(known.cast('int')).alias('informed'),
                F.sum((F.col('competition') == 'uno').cast('int')).alias('single_bidder'),
                F.sum(F.when(F.col('competition') == 'uno', F.col('tender_amount_pen'))).alias('single_bidder_amount_pen'))
                .withColumn('dimension', F.lit(dimension))
                .withColumnRenamed(dimension, 'value'))
            if not by_year:
                frame = frame.withColumn('year', F.lit('Todos'))
            frames.append(frame.select('dimension', 'value', F.col('year').cast('string').alias('year'), 'processes',
                                       'informed', 'single_bidder', 'single_bidder_amount_pen'))
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return (result
            .withColumn('single_bidder_rate', F.col('single_bidder') / F.col('informed'))
            .withColumn('missing_share', 1 - F.col('informed') / F.col('processes'))
            .orderBy('dimension', 'year', F.desc('single_bidder_rate')))
