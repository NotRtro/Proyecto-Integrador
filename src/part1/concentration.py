"""Concentración del gasto y de las adjudicaciones (Pregunta Q1).

Motor: Spark (DataFrames y funciones de ventana). Todas las métricas oficiales se
calculan aquí; la API solo filtra los resultados.

Índice de Herfindahl-Hirschman (HHI) de un grupo con participaciones s_i:
    HHI = 10 000 · Σ s_i²   (0 = atomizado, 10 000 = un solo actor)
Umbrales usuales de las agencias de competencia: < 1 500 no concentrado,
1 500-2 500 moderado, > 2 500 alto.

Cautelas que se publican con los resultados:
- Los consorcios usan un identificador nuevo por cada contrato y ocultan a sus
  integrantes; incluirlos como proveedores distintos subestima la concentración.
  Por eso el HHI de proveedores se informa con y sin consorcios.
- Una entidad con un solo proveedor tiene HHI 10 000 por definición: el HHI por
  entidad solo se calcula con un mínimo de adjudicaciones (MIN_AWARDS).
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

MIN_AWARDS = 5
HIGH_HHI = 2500


def concentration(df, groups, entity, value, top=10):
    """HHI, participación del top-N, número de actores y total por grupo.

    df: filas con columnas `groups`, `entity` y `value` (se suman por entidad).
    """
    by_entity = (
        df.filter(F.col(value).isNotNull() & (F.col(value) > 0))
        .groupBy(*groups, entity)
        .agg(F.sum(value).alias('value'))
    )
    group = Window.partitionBy(*groups)
    ranked = by_entity.withColumn('share', F.col('value') / F.sum('value').over(group)).withColumn(
        'rank', F.row_number().over(group.orderBy(F.desc('value'), entity))
    )
    metrics = ranked.groupBy(*groups).agg(
        F.sum('value').alias('total'),
        F.count('*').alias('entities'),
        (F.sum(F.col('share') ** 2) * 10000).alias('hhi'),
        F.sum(F.when(F.col('rank') <= top, F.col('share')).otherwise(0)).alias(f'top{top}_share'),
        F.max('share').alias('top1_share'),
    )
    return metrics, ranked


def hhi_band(column):
    return F.when(F.col(column) > HIGH_HHI, 'alta').when(F.col(column) >= 1500, 'moderada').otherwise('baja')


def supplier_frame(suppliers, processes):
    """Proveedores con el departamento y nivel de gobierno del comprador."""
    context = processes.select('ocid', 'department', 'buyer_level', 'buyer_name')
    return suppliers.join(context, 'ocid', 'left').withColumn(
        'department', F.coalesce('department', F.lit('NO INFORMADO'))
    )


def market_concentration(processes, suppliers):
    """Concentración nacional por año: entidades (gasto adjudicado) y proveedores (monto atribuido)."""
    rows = []
    buyers, _ = concentration(processes, ['year'], 'buyer_id', 'award_amount_pen')
    rows.append(buyers.withColumn('scope', F.lit('entidades')))
    all_suppliers, _ = concentration(suppliers, ['year'], 'supplier_id', 'allocated_amount_pen')
    rows.append(all_suppliers.withColumn('scope', F.lit('proveedores (todos)')))
    no_consortia, _ = concentration(
        suppliers.filter("supplier_type != 'consorcio'"), ['year'], 'supplier_id', 'allocated_amount_pen'
    )
    rows.append(no_consortia.withColumn('scope', F.lit('proveedores (sin consorcios)')))
    result = rows[0]
    for frame in rows[1:]:
        result = result.unionByName(frame)
    return result.withColumn('band', hhi_band('hhi')).orderBy('scope', 'year')


def buyer_supplier_concentration(suppliers):
    """Concentración de proveedores dentro de cada entidad compradora y año (dependencia de proveedores)."""
    awards = suppliers.groupBy('year', 'buyer_id').agg(F.countDistinct('ocid', 'award_id').alias('awards'))
    metrics, ranked = concentration(suppliers, ['year', 'buyer_id'], 'supplier_id', 'allocated_amount_pen')
    top = (
        ranked.filter('rank = 1')
        .join(
            suppliers.select('supplier_id', 'supplier_name', 'supplier_type').dropDuplicates(['supplier_id']),
            'supplier_id',
            'left',
        )
        .select(
            'year',
            'buyer_id',
            F.col('supplier_name').alias('top_supplier'),
            F.col('supplier_type').alias('top_supplier_type'),
        )
    )
    buyers = suppliers.select('buyer_id', 'buyer_name', 'buyer_level', 'department').dropDuplicates(['buyer_id'])
    return (
        metrics.join(awards, ['year', 'buyer_id'])
        .join(top, ['year', 'buyer_id'], 'left')
        .join(buyers, 'buyer_id', 'left')
        .filter(F.col('awards') >= MIN_AWARDS)
        .withColumn('band', hhi_band('hhi'))
    )


def department_supplier_concentration(suppliers):
    """HHI de proveedores por departamento del comprador y año, con y sin consorcios."""
    everyone, _ = concentration(suppliers, ['year', 'department'], 'supplier_id', 'allocated_amount_pen')
    without, _ = concentration(
        suppliers.filter("supplier_type != 'consorcio'"), ['year', 'department'], 'supplier_id', 'allocated_amount_pen'
    )
    without = without.select(
        'year',
        'department',
        F.col('hhi').alias('hhi_sin_consorcios'),
        F.col('top10_share').alias('top10_share_sin_consorcios'),
    )
    return everyone.join(without, ['year', 'department'], 'left').withColumn('band', hhi_band('hhi'))


def buyer_hhi_summary(buyer_hhi):
    """Distribución del HHI por entidad: cuántas dependen de pocos proveedores, por año y nivel de gobierno."""
    return (
        buyer_hhi.groupBy('year', 'buyer_level')
        .agg(
            F.count('*').alias('buyers'),
            F.expr('percentile_approx(hhi, 0.5)').alias('median_hhi'),
            F.avg(F.when(F.col('hhi') > HIGH_HHI, 1).otherwise(0)).alias('high_share'),
            F.avg('top1_share').alias('mean_top1_share'),
        )
        .orderBy('year', 'buyer_level')
    )


def top_rankings(df, groups, entity, labels, value, n=25):
    """Top-n entidades por grupo, con su participación (para los gráficos de ranking)."""
    _, ranked = concentration(df, groups, entity, value, top=n)
    names = df.select(entity, *labels).dropDuplicates([entity])
    return ranked.filter(F.col('rank') <= n).join(names, entity, 'left').orderBy(*groups, 'rank')
