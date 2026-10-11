"""Agregaciones de la Parte I en estilo DataFrame (declarativo, optimizado por Catalyst).

Motor: Spark DataFrames. Cada función tiene su gemela en aggregations_rdd.py y ambas
deben dar el mismo resultado; benchmark.py compara su claridad, plan y tiempo.
"""

from pyspark.sql import functions as F


def category_amount(processes, suppliers=None):
    """Monto convocado total por categoría."""
    return processes.groupBy('category').agg(F.sum('tender_amount_pen').alias('value'))


def buyer_count(processes, suppliers=None):
    """Número de procesos por entidad compradora."""
    return processes.groupBy('buyer_id').agg(F.count('*').alias('value'))


def department_supplier_hhi(processes, suppliers):
    """HHI de proveedores por departamento del comprador: join + dos agregaciones.

    HHI = 10 000 · Σ v_i² / (Σ v_i)², con v_i el monto atribuido al proveedor i.
    """
    by_supplier = (
        suppliers.filter(F.col('allocated_amount_pen') > 0)
        .join(processes.select('ocid', 'department'), 'ocid')
        .groupBy('department', 'supplier_id')
        .agg(F.sum('allocated_amount_pen').alias('v'))
    )
    return by_supplier.groupBy('department').agg((F.sum(F.col('v') ** 2) / F.sum('v') ** 2 * 10000).alias('value'))
