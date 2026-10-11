"""Las mismas agregaciones de la Parte I en estilo MapReduce sobre RDDs.

Motor: Spark RDDs. Se describe paso a paso cómo se mueve cada dato (map -> clave,
reduceByKey -> combinación local y luego shuffle). Las funciones lambda corren en
procesos Python de los ejecutores: cada fila se serializa entre la JVM y Python, y
Catalyst no puede optimizar nada de lo que ocurre dentro de ellas.
"""


def category_amount(processes, suppliers=None):
    """Monto convocado total por categoría (los nulos cuentan como 0 en la suma)."""
    return (processes.select('category', 'tender_amount_pen').rdd
            .map(lambda row: (row[0], row[1] or 0.0))
            .reduceByKey(lambda a, b: a + b))


def buyer_count(processes, suppliers=None):
    """Número de procesos por entidad compradora: el clásico conteo de palabras."""
    return (processes.select('buyer_id').rdd
            .map(lambda row: (row[0], 1))
            .reduceByKey(lambda a, b: a + b))


def department_supplier_hhi(processes, suppliers):
    """HHI de proveedores por departamento: join por ocid y dos reduceByKey.

    En lugar de agrupar todos los montos de un departamento (groupByKey), se acumulan
    (Σv, Σv²) con reduceByKey, que combina localmente antes del shuffle.
    """
    departments = processes.select('ocid', 'department').rdd.map(lambda row: (row[0], row[1]))
    amounts = (suppliers.select('ocid', 'supplier_id', 'allocated_amount_pen').rdd
               .filter(lambda row: row[2] is not None and row[2] > 0)
               .map(lambda row: (row[0], (row[1], row[2]))))
    by_supplier = (amounts.join(departments)                                  # (ocid, ((proveedor, v), dpto))
                   .map(lambda kv: ((kv[1][1], kv[1][0][0]), kv[1][0][1]))    # ((dpto, proveedor), v)
                   .reduceByKey(lambda a, b: a + b))
    return (by_supplier
            .map(lambda kv: (kv[0][0], (kv[1], kv[1] ** 2)))                  # (dpto, (v, v²))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda s: s[1] / s[0] ** 2 * 10000))
