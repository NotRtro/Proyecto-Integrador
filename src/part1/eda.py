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
- Pregunta Q2: tasa de procesos con un solo postor por cada dimensión, qué cuenta el
  número de postores (bidders_by_modality) y la tasa por modalidad a igual mezcla de
  categoría, departamento, año y monto (adjusted_competition).
"""

from pyspark.sql import functions as F

from src.processing.model import COMPETITIVE_MODALITIES, DIRECT_MODALITY

NOT_INFORMED = 'NO INFORMADO'
CATEGORICAL = [
    'department',
    'category',
    'buyer_id',
    'buyer_name',
    'buyer_level',
    'method',
    'method_details',
    'modality',
]
CUBE_DIMENSIONS = [
    'year',
    'month',
    'department',
    'category',
    'modality',
    'buyer_level',
    'buyer_id',
    'buyer_name',
    'competition',
    'amount_band',
]


def amounts():
    """Medidas comunes de cada agregación (función: una Column necesita SparkSession activa)."""
    return [
        F.count('*').alias('processes'),
        F.sum('tender_amount_pen').alias('tender_amount_pen'),
        F.sum('award_amount_pen').alias('award_amount_pen'),
    ]


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
    return (
        positive.withColumn('log10_bin', bin_start)
        .groupBy('year', 'category', 'log10_bin')
        .agg(F.count('*').alias('processes'), F.sum('tender_amount_pen').alias('tender_amount_pen'))
        .withColumn('bin_from_pen', F.pow(F.lit(10.0), 'log10_bin'))
        .withColumn('bin_to_pen', F.pow(F.lit(10.0), F.col('log10_bin') + LOG_BIN))
        .orderBy('year', 'category', 'log10_bin')
    )


def amount_percentiles(processes):
    """Percentiles del monto convocado por año y categoría (percentile_approx, distribuido)."""
    quantiles = F.expr(f"percentile_approx(tender_amount_pen, array({', '.join(map(str, PERCENTILES))}), 10000)")
    grouped = (
        processes.filter(F.col('tender_amount_pen').isNotNull())
        .groupBy('year', 'category')
        .agg(F.count('*').alias('processes'), F.avg('tender_amount_pen').alias('mean'), quantiles.alias('q'))
    )
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
        F.max('tender_amount_pen').alias('max_tender_amount'),
    )


def bidders_by_modality(processes):
    """¿Qué cuenta tender.numberOfTenderers? Distribución del número de postores por modalidad.

    El OCDS del OECE no publica ofertas (bids) y su documentación no define el campo. Una
    mediana de 9 en Adjudicación Simplificada y máximos de cientos son más compatibles con
    participantes registrados que con ofertas presentadas. Es una hipótesis: el estándar
    OCDS define el campo por las ofertas. Si se cumple, "un postor" significa "un solo
    participante" y ese proceso recibió una oferta o ninguna.
    """
    known = processes.filter(F.col('bidders').isNotNull() & (F.col('bidders') >= 1))
    return (
        known.groupBy('modality')
        .agg(
            F.count('*').alias('processes'),
            F.expr('percentile_approx(bidders, 0.5)').alias('median'),
            F.expr('percentile_approx(bidders, 0.9)').alias('p90'),
            F.max('bidders').alias('max'),
            F.avg((F.col('bidders') == 1).cast('int')).alias('single_share'),
            F.avg((F.col('bidders') >= 10).cast('int')).alias('ten_or_more_share'),
        )
        .orderBy(F.desc('processes'))
    )


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
            frame = (
                processes.groupBy(*keys)
                .agg(
                    F.count('*').alias('processes'),
                    F.sum(known.cast('int')).alias('informed'),
                    F.sum((F.col('competition') == 'uno').cast('int')).alias('single_bidder'),
                    F.sum(F.when(F.col('competition') == 'uno', F.col('tender_amount_pen'))).alias(
                        'single_bidder_amount_pen'
                    ),
                )
                .withColumn('dimension', F.lit(dimension))
                .withColumnRenamed(dimension, 'value')
            )
            if not by_year:
                frame = frame.withColumn('year', F.lit('Todos'))
            frames.append(
                frame.select(
                    'dimension',
                    'value',
                    F.col('year').cast('string').alias('year'),
                    'processes',
                    'informed',
                    'single_bidder',
                    'single_bidder_amount_pen',
                )
            )
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return (
        result.withColumn('single_bidder_rate', F.col('single_bidder') / F.col('informed'))
        .withColumn('missing_share', 1 - F.col('informed') / F.col('processes'))
        .orderBy('dimension', 'year', F.desc('single_bidder_rate'))
    )


STRATA = ['category', 'department', 'year', 'amount_band']
MIN_STRATUM = 20  # procesos informados por lado para comparar dentro de un estrato


def adjusted_competition(processes):
    """Q2: ¿la modalidad explica el postor único, o lo explica el tipo de compra?

    Estandarización directa: la tasa de cada modalidad se recalcula con la mezcla de
    estratos (categoría, departamento, año y banda de monto) de las modalidades
    competitivas. Spark reduce los procesos a un cubo pequeño y la ponderación se hace en
    el driver. `strata_coverage` es la parte de esa mezcla que la modalidad llega a cubrir:
    si es baja, la tasa ajustada se apoya en pocos estratos.

    Devuelve las filas por modalidad y la comparación estrato a estrato entre contratación
    directa y modalidades competitivas.
    """
    known = processes.filter(F.col('competition') != 'sin dato')
    for column in ['category', 'department', 'modality']:
        known = known.withColumn(column, F.coalesce(F.col(column), F.lit(NOT_INFORMED)))
    cube = (
        known.groupBy('modality', *STRATA)
        .agg(F.count('*').alias('informed'), F.sum((F.col('competition') == 'uno').cast('int')).alias('single'))
        .toPandas()
    )
    cube['group'] = cube.modality.map(
        lambda m: 'competitiva' if m in COMPETITIVE_MODALITIES else 'directa' if m == DIRECT_MODALITY else 'otra'
    )
    reference = cube[cube.group == 'competitiva'].groupby(STRATA).informed.sum()
    weights = reference / reference.sum()

    def adjusted(part):
        by_stratum = part.groupby(STRATA)[['informed', 'single']].sum()
        shared = weights.reindex(by_stratum.index).dropna()
        if shared.empty:
            return None, 0.0
        rates = (by_stratum.single / by_stratum.informed)[shared.index]
        return float((rates * shared).sum() / shared.sum()), float(shared.sum())

    rows = []
    for level, key in [('grupo', 'group'), ('modalidad', 'modality')]:
        for name, part in cube.groupby(key):
            rate, coverage = adjusted(part)
            rows.append(
                dict(
                    level=level,
                    name=name,
                    group=part.group.iloc[0],
                    informed=int(part.informed.sum()),
                    single_bidder=int(part.single.sum()),
                    raw_rate=float(part.single.sum() / part.informed.sum()),
                    adjusted_rate=rate,
                    strata_coverage=coverage,
                )
            )
    rows.sort(key=lambda r: (r['level'], -r['informed']))
    by_group = cube.groupby(['group', *STRATA])[['informed', 'single']].sum().reset_index()
    sides = by_group[by_group.group == 'directa'].merge(
        by_group[by_group.group == 'competitiva'], on=STRATA, suffixes=('_direct', '_competitive')
    )
    sides = sides[(sides.informed_direct >= MIN_STRATUM) & (sides.informed_competitive >= MIN_STRATUM)]
    gap = sides.single_direct / sides.informed_direct - sides.single_competitive / sides.informed_competitive
    persistence = dict(
        strata=STRATA,
        min_processes_per_side=MIN_STRATUM,
        comparable_strata=int(len(sides)),
        strata_where_direct_is_higher=int((gap > 0).sum()),
        median_gap=float(gap.median()) if len(gap) else None,
        min_gap=float(gap.min()) if len(gap) else None,
        direct_processes_compared=int(sides.informed_direct.sum()),
        competitive_modalities=COMPETITIVE_MODALITIES,
        note='Cada estrato compara contratación directa y modalidades competitivas con la misma categoría, '
        'departamento del comprador, año y banda de monto. La tasa ajustada pondera cada estrato con su peso '
        'entre las modalidades competitivas. Describe una asociación; no controla variables no observadas.',
    )
    return rows, persistence
