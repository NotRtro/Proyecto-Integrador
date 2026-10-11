"""¿Qué se pierde si la Q1 se responde con una muestra? (evidencia de docs/DecisionesTecnicas.md)

Motor: Spark. Se repite el HHI por entidad sobre muestras del 30 % y del 10 % de los
procesos (por hash del ocid, reproducible) y se compara con el resultado sobre todos:

- entidades evaluables: con menos adjudicaciones, muchas entidades no alcanzan el mínimo
  de MIN_AWARDS y desaparecen del análisis;
- HHI mediano: con pocas adjudicaciones por entidad el HHI se infla (con dos proveedores
  ya parece concentrado), así que la muestra exagera la dependencia;
- proveedor principal: en qué proporción de entidades la muestra identifica al mismo
  proveedor principal que el total.

Si la muestra cambia la respuesta, no sirve para esta pregunta aunque ahorre tiempo.
"""

from pyspark.sql import functions as F

from src.config import SEED
from src.part1.concentration import buyer_supplier_concentration

FRACTIONS = [1.0, 0.3, 0.1]


def by_ocid_hash(df, fraction):
    if fraction >= 1:
        return df
    bucket = F.abs(F.xxhash64('ocid', F.lit(SEED))) % 1000
    return df.filter(bucket < int(fraction * 1000))


def sampling_effect(suppliers):
    full = buyer_supplier_concentration(suppliers).select('year', 'buyer_id', 'hhi', 'top_supplier').cache()
    rows = []
    for fraction in FRACTIONS:
        sample = full if fraction >= 1 else buyer_supplier_concentration(by_ocid_hash(suppliers, fraction))
        sample = sample.select('year', 'buyer_id', 'hhi', F.col('top_supplier').alias('sample_top'))
        joined = sample.join(full.select('year', 'buyer_id', 'top_supplier'), ['year', 'buyer_id'], 'left')
        stats = joined.groupBy('year').agg(
            F.count('*').alias('buyers_evaluable'),
            F.expr('percentile_approx(hhi, 0.5)').alias('median_hhi'),
            F.avg(F.when(F.col('hhi') > 2500, 1).otherwise(0)).alias('high_share'),
            F.avg((F.col('sample_top') == F.col('top_supplier')).cast('int')).alias('same_top_supplier'),
        )
        rows.extend(dict(fraction=fraction, **r.asDict()) for r in stats.orderBy('year').collect())
    full.unpersist()
    return rows
