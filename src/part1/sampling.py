"""¿Qué se pierde si la Q1 se responde con una muestra? (evidencia de docs/DecisionesTecnicas.md)

Motor: Spark. Se repite el HHI por entidad sobre muestras del 30 % y del 10 % y se
compara con el resultado sobre todos los datos. Dos diseños de muestra, ambos
reproducibles por hash:

- aleatorio: procesos al azar (hash del ocid). Cada entidad pierde adjudicaciones.
- conglomerados: entidades completas al azar (hash del buyer_id), con todos sus procesos.
  Es la alternativa fuerte: el HHI de cada entidad elegida es exacto.

Métricas por diseño, fracción y año:
- entidades evaluables: las que alcanzan el mínimo de MIN_AWARDS adjudicaciones;
- HHI mediano y proporción con HHI > 2 500: con pocas adjudicaciones por entidad el HHI
  se infla (con dos proveedores ya parece concentrado), así que el diseño aleatorio
  exagera la dependencia;
- proveedor principal: en qué proporción de entidades la muestra identifica al mismo
  proveedor principal que el total;
- detección de entidades concentradas: de las entidades con HHI > 2 500 en el total, qué
  proporción señala la muestra (recall), y de las que señala la muestra, cuántas lo son
  de verdad (precisión). La Q1 pregunta justamente cuáles son.

Si la muestra cambia la respuesta, no sirve para esta pregunta aunque ahorre tiempo.
"""

from pyspark.sql import functions as F

from src.config import SEED
from src.part1.concentration import buyer_supplier_concentration

FRACTIONS = [1.0, 0.3, 0.1]
DESIGNS = {'aleatorio': 'ocid', 'conglomerados': 'buyer_id'}
HIGH_HHI = 2500


def by_hash(df, column, fraction):
    """Filas cuyo `column` cae en la fracción indicada, según un hash reproducible."""
    if fraction >= 1:
        return df
    bucket = F.abs(F.xxhash64(column, F.lit(SEED))) % 1000
    return df.filter(bucket < int(fraction * 1000))


def compare(sample, full):
    """Resumen por año de una muestra frente al total (ambos: HHI por entidad)."""
    sample = sample.select(
        'year', 'buyer_id', 'hhi', F.col('top_supplier').alias('sample_top'), (F.col('hhi') > HIGH_HHI).alias('flag')
    )
    joined = sample.join(
        full.select('year', 'buyer_id', 'top_supplier', (F.col('hhi') > HIGH_HHI).alias('truly_high')),
        ['year', 'buyer_id'],
        'left',
    )
    stats = joined.groupBy('year').agg(
        F.count('*').alias('buyers_evaluable'),
        F.expr('percentile_approx(hhi, 0.5)').alias('median_hhi'),
        F.avg(F.col('flag').cast('int')).alias('high_share'),
        F.avg((F.col('sample_top') == F.col('top_supplier')).cast('int')).alias('same_top_supplier'),
        F.sum((F.col('flag') & F.col('truly_high')).cast('int')).alias('high_detected'),
        F.sum(F.col('flag').cast('int')).alias('high_flagged'),
    )
    truth = full.groupBy('year').agg(F.sum((F.col('hhi') > HIGH_HHI).cast('int')).alias('high_full'))
    return (
        stats.join(truth, 'year')
        .withColumn('high_recall', F.col('high_detected') / F.col('high_full'))
        .withColumn('high_precision', F.col('high_detected') / F.col('high_flagged'))
        .drop('high_detected', 'high_flagged', 'high_full')
        .orderBy('year')
        .collect()
    )


def sampling_effect(suppliers):
    full = buyer_supplier_concentration(suppliers).select('year', 'buyer_id', 'hhi', 'top_supplier').cache()
    rows = [dict(design='todos', fraction=1.0, **r.asDict()) for r in compare(full, full)]
    for design, column in DESIGNS.items():
        for fraction in FRACTIONS:
            if fraction >= 1:
                continue
            sample = buyer_supplier_concentration(by_hash(suppliers, column, fraction))
            rows.extend(dict(design=design, fraction=fraction, **r.asDict()) for r in compare(sample, full))
    full.unpersist()
    return rows
