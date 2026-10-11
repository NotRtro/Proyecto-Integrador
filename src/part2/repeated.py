"""Requerimientos casi idénticos: ¿quién los repite y cuándo? (Pregunta Q3).

Motor: Spark (joins y agregaciones sobre los pares verificados del corpus completo).

Cada par con Jaccard >= umbral se clasifica:
- misma entidad, <= 30 días: el mismo requerimiento convocado dos veces en poco tiempo.
  Puede ser una reconvocatoria (proceso desierto o nulo), un error de registro o una
  posible división de la compra; distinguirlos exige revisar el expediente.
- misma entidad, > 30 días: compras recurrentes (mantenimiento, combustible, raciones).
- entidades distintas: el mismo requerimiento en varias entidades; señal de bienes y
  servicios estandarizables (compras corporativas o catálogos electrónicos).

Reconvocatoria: la nomenclatura OECE termina en el número de convocatoria
(AS-SM-12-2024-MDCH/CS-1, ...-2). Si dos procesos de la misma entidad comparten la
nomenclatura salvo ese número, se marcan como reconvocatoria.

Ninguna categoría implica irregularidad: son señales para priorizar revisión.
"""
from pyspark.sql import functions as F

SHORT_WINDOW_DAYS = 30
CATEGORIES = ['misma entidad, <= 30 días', 'misma entidad, > 30 días', 'entidades distintas']


def classify(similar, processes):
    """Pares verificados con contexto de ambos procesos y su categoría."""
    context = processes.select('ocid', 'buyer_id', 'buyer_name', 'buyer_level', 'department', 'category', 'method',
                               'effective_date', 'year', 'tender_code', 'tender_amount_pen')
    left = context.select(*[F.col(c).alias(f'left_{c}') if c != 'ocid' else F.col(c).alias('left') for c in context.columns])
    right = context.select(*[F.col(c).alias(f'right_{c}') if c != 'ocid' else F.col(c).alias('right') for c in context.columns])
    days = F.abs(F.datediff(F.to_date('left_effective_date'), F.to_date('right_effective_date')))
    base_code = lambda c: F.regexp_replace(F.col(c), r'-\d+$', '')  # noqa: E731 (nomenclatura sin el número final)
    same_buyer = F.col('left_buyer_id') == F.col('right_buyer_id')
    return (similar.join(left, 'left').join(right, 'right')
            .withColumn('days_apart', days)
            .withColumn('same_buyer', same_buyer)
            .withColumn('reconvocation', same_buyer & (base_code('left_tender_code') == base_code('right_tender_code')))
            .withColumn('kind', F.when(same_buyer & (F.col('days_apart') <= SHORT_WINDOW_DAYS), CATEGORIES[0])
                        .when(same_buyer, CATEGORIES[1]).otherwise(CATEGORIES[2])))


def involved_processes(pairs, keys):
    """Procesos distintos que participan en algún par, por grupo.

    Los pares crecen de forma cuadrática: n procesos idénticos forman n(n-1)/2 pares.
    El número de procesos involucrados es la medida comparable entre grupos.
    """
    sides = pairs.select(*keys, F.explode(F.array('left', 'right')).alias('ocid'))
    return sides.groupBy(*keys).agg(F.countDistinct('ocid').alias('processes_involved'))


def summary(pairs):
    """Pares y procesos involucrados por categoría y año, reconvocatorias y monto."""
    with_year = pairs.withColumn('year', F.greatest('left_year', 'right_year'))
    totals = with_year.groupBy('kind', 'year').agg(
        F.count('*').alias('pairs'),
        F.sum(F.col('reconvocation').cast('int')).alias('reconvocation_pairs'),
        F.countDistinct('left_buyer_id').alias('buyers'),
        F.avg('jaccard').alias('mean_jaccard'))
    return totals.join(involved_processes(with_year, ['kind', 'year']), ['kind', 'year']).orderBy('kind', 'year')


def top_buyers(pairs, n=30):
    """Entidades con más procesos repetidos en <= 30 días, sin contar reconvocatorias."""
    short = pairs.filter((F.col('kind') == CATEGORIES[0]) & ~F.col('reconvocation'))
    keys = ['left_buyer_id', 'left_buyer_name', 'left_buyer_level', 'left_department']
    stats = short.groupBy(*keys).agg(F.count('*').alias('pairs'), F.avg('jaccard').alias('mean_jaccard'),
                                     F.avg('days_apart').alias('mean_days_apart'))
    return (stats.join(involved_processes(short, keys), keys)
            .orderBy(F.desc('processes_involved')).limit(n))


def case_candidates(pairs, features, per_kind=5):
    """Pares de ejemplo por categoría para que el equipo los revise en SEACE.

    Se eligen los de mayor monto conjunto (más relevantes de revisar) y se incluyen los
    textos y la nomenclatura para buscarlos en el buscador público del SEACE.
    """
    texts = features.select('ocid', 'text')
    ranked = pairs.withColumn('amount', F.coalesce('left_tender_amount_pen', F.lit(0)) + F.coalesce('right_tender_amount_pen', F.lit(0)))
    rows = []
    for kind in CATEGORIES:
        chosen = (ranked.filter((F.col('kind') == kind) & ~F.col('reconvocation'))
                  .orderBy(F.desc('amount')).limit(per_kind)
                  .join(texts.withColumnRenamed('ocid', 'left').withColumnRenamed('text', 'left_text'), 'left')
                  .join(texts.withColumnRenamed('ocid', 'right').withColumnRenamed('text', 'right_text'), 'right'))
        rows.extend(r.asDict() for r in chosen.collect())
    return rows
