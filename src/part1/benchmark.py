"""Comparación RDD (MapReduce) frente a DataFrame (Parte I).

Motor: Spark. Para cada operación se mide:
- equivalencia de resultados (si difieren, la corrida falla);
- tiempo: una ejecución de calentamiento y tres medidas (se reporta la mediana);
- plan: operadores físicos del DataFrame y número de shuffles de cada enfoque,
  leídos del plan real (no escritos a mano);
- claridad: el código fuente de ambas versiones y sus líneas efectivas, para
  mostrarlos lado a lado en el reporte.
"""
import ast
import inspect
import re
import statistics
import textwrap
import time

from src.part1 import aggregations_dataframe as dataframe_ops
from src.part1 import aggregations_rdd as rdd_ops

OPERATIONS = ['category_amount', 'buyer_count', 'department_supplier_hhi']
DESCRIPTIONS = {
    'category_amount': 'Monto convocado total por categoría',
    'buyer_count': 'Número de procesos por entidad',
    'department_supplier_hhi': 'HHI de proveedores por departamento (join + dos agregaciones)',
}
REPETITIONS = 3


def code_lines(function):
    """Líneas de código efectivas (sin docstring, comentarios ni líneas vacías)."""
    source = textwrap.dedent(inspect.getsource(function))
    body = ast.parse(source).body[0].body
    if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], 'value', None), ast.Constant):
        body = body[1:]
    lines = source.splitlines()[body[0].lineno - 1:body[-1].end_lineno]
    return sum(1 for line in lines if line.strip() and not line.strip().startswith('#'))


def dataframe_plan(query):
    plan = query._jdf.queryExecution().executedPlan().toString()
    operators = re.findall(r'(?:^|[+:]- )\s*(\w+)', plan, flags=re.MULTILINE)
    return plan, operators, len(re.findall(r'\bExchange\b', plan))


def rdd_plan(rdd):
    plan = rdd.toDebugString().decode()
    return plan, len(re.findall(r'ShuffledRDD', plan))


def normalize(rows):
    return {row[0]: float(row[1] or 0) for row in rows}


def assert_equivalent(reference, other, operation):
    if reference.keys() != other.keys():
        raise AssertionError(f'{operation}: claves distintas entre RDD y DataFrame')
    for key, value in reference.items():
        if abs(value - other[key]) > 1e-6 * max(1.0, abs(value)):
            raise AssertionError(f'{operation}: {key} difiere ({value} vs {other[key]})')


def timed(build, processes, suppliers):
    start = time.perf_counter()
    rows = build(processes, suppliers).collect()
    return rows, time.perf_counter() - start


def run_benchmark(processes, suppliers):
    results, plans, snippets = [], {}, {}
    partitions = processes.rdd.getNumPartitions()
    for operation in OPERATIONS:
        reference = None
        snippets[operation] = dict(description=DESCRIPTIONS[operation])
        for engine, module in [('DataFrame', dataframe_ops), ('RDD', rdd_ops)]:
            build = getattr(module, operation)
            rows, warmup = timed(build, processes, suppliers)
            values = normalize(rows)
            if reference is None:
                reference = values
            else:
                assert_equivalent(reference, values, operation)
            times = [timed(build, processes, suppliers)[1] for _ in range(REPETITIONS)]
            query = build(processes, suppliers)
            if engine == 'DataFrame':
                plan, operators, shuffles = dataframe_plan(query)
            else:
                (plan, shuffles), operators = rdd_plan(query), None
            plans[f'{operation}_{engine}'] = plan
            function = getattr(module, operation)
            snippets[operation][engine] = dict(code=textwrap.dedent(inspect.getsource(function)),
                                               lines=code_lines(function))
            results.append(dict(
                operation=operation, description=DESCRIPTIONS[operation], engine=engine,
                warmup_seconds=warmup, median_seconds=statistics.median(times),
                **{f'run{i + 1}': t for i, t in enumerate(times)},
                input_partitions=partitions, shuffles=shuffles, code_lines=code_lines(function),
                operators=' → '.join(reversed(operators)) if operators else None,
                groups=len(values), equivalent=True))
    return results, plans, snippets
