"""Experimento de particionamiento y su efecto en el plan de ejecución (Parte I, I-01).

Motor: Spark. Cuatro preguntas, cada una medida en el clúster:

1. Lectura: ¿cuántas particiones produce leer los .jsonl.gz? Gzip no es divisible,
   así que Spark crea una partición por archivo sin importar cuántos núcleos haya.
2. Partition pruning: `processes` está escrito con partitionBy(year, month). Un filtro
   sobre la columna de partición (year = 2024) le permite a Spark leer solo esas
   carpetas (PartitionFilters en el plan). El mismo filtro expresado con otra columna
   (effective_date dentro de 2024) selecciona las mismas filas pero obliga a leer todo.
3. spark.sql.shuffle.partitions: cuántas tareas crea un shuffle. Pocas tareas grandes
   frente a muchas pequeñas, con AQE apagado (el valor fijo) y encendido (Spark
   fusiona particiones pequeñas en tiempo de ejecución).
4. repartition por clave: no acelera una agregación aislada (agrega un shuffle de filas
   completas), pero si se cachea, varias agregaciones por la misma clave lo reutilizan
   y Spark omite sus propios shuffles.
"""
import re
import statistics
import time
from pathlib import Path

from pyspark.sql import functions as F

from src.config import PROCESSED

REPETITIONS = 3
SHUFFLE_PARTITIONS = [8, 64, 200]


def measure(action):
    action()  # calentamiento: carga de metadatos y JIT de la JVM
    times = []
    for _ in range(REPETITIONS):
        start = time.perf_counter()
        action()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def plan_text(df):
    return df._jdf.queryExecution().executedPlan().toString()


def exchanges(df):
    """Shuffles que ejecutará la consulta.

    El texto del plan repite, debajo de cada InMemoryRelation, el plan que llenó la
    caché (con su propio Exchange). Esos shuffles ya ocurrieron y no se repiten, así
    que se ignora todo el subárbol de InMemoryRelation.
    """
    count, cached_indent = 0, None
    for line in plan_text(df).splitlines():
        indent = len(line) - len(line.lstrip(' +-:'))
        if cached_indent is not None and indent > cached_indent:
            continue
        cached_indent = indent if 'InMemoryRelation' in line else None
        count += bool(re.search(r'\bExchange\b', line))
    return count


def scan_filters(df):
    """Filtros de partición (carpetas descartadas) y filtros empujados a Parquet.

    Spark recorta los textos largos del plan con "...", por eso la captura termina en
    "], " o en "...". Un PushedFilter permite saltar grupos de filas por sus
    estadísticas min/max, pero Spark igual abre cada archivo para leerlas; un
    PartitionFilter descarta carpetas completas sin abrirlas.
    """
    plan = plan_text(df)
    found = {}
    for key in ['PartitionFilters', 'PushedFilters']:
        match = re.search(key + r': \[(.*?)(?:\], |\.\.\.)', plan)
        found[key] = match.group(1) if match else ''
    return dict(partition_filters=found['PartitionFilters'], pushed_filters=found['PushedFilters'])


def parquet_folders(table, **partition):
    """Carpetas año/mes que existen y las que coinciden con el filtro de partición."""
    folders = [p for p in (PROCESSED / table).glob('year=*/month=*') if p.is_dir()]
    matching = [p for p in folders if all(f'{k}={v}' in p.as_posix() for k, v in partition.items())]
    return len(folders), len(matching)


def input_partitions(spark, raw_paths):
    raw = spark.read.text([str(Path(p).resolve()) for p in raw_paths])
    target = int(spark.conf.get('spark.default.parallelism', '8')) * 2
    return dict(files=len(raw_paths), read_partitions=raw.rdd.getNumPartitions(), after_repartition=target,
                explanation='Gzip no es divisible: una partición por archivo. build_processes reparticiona '
                            f'a {target} antes de parsear para usar todos los núcleos.')


def pruning(spark, year):
    processes = spark.read.parquet(str(PROCESSED / 'processes'))
    by_partition = processes.filter(F.col('year') == year)
    by_date = processes.filter(F.col('effective_date').between(f'{year}-01-01', f'{year}-12-31'))
    total, matching = parquet_folders('processes', year=year)
    rows = by_partition.count()
    if rows != by_date.count():
        raise AssertionError('Los dos filtros deberían seleccionar las mismas filas')
    return dict(
        year=year, rows=rows, folders_total=total, folders_read_with_pruning=matching,
        partition_column=dict(seconds=measure(by_partition.count), **scan_filters(by_partition)),
        other_column=dict(seconds=measure(by_date.count), **scan_filters(by_date)))


def shuffle_partitions(spark):
    processes = spark.read.parquet(str(PROCESSED / 'processes')).cache()
    processes.count()
    original = {k: spark.conf.get(k) for k in ['spark.sql.shuffle.partitions', 'spark.sql.adaptive.enabled']}
    rows = []
    try:
        for adaptive in ['false', 'true']:
            spark.conf.set('spark.sql.adaptive.enabled', adaptive)
            for partitions in SHUFFLE_PARTITIONS:
                spark.conf.set('spark.sql.shuffle.partitions', str(partitions))
                query = processes.groupBy('buyer_id', 'year').agg(F.sum('award_amount_pen'))
                seconds = measure(query.collect)
                rows.append(dict(adaptive=adaptive == 'true', shuffle_partitions=partitions, seconds=seconds,
                                 result_partitions=query.rdd.getNumPartitions()))
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)
        processes.unpersist()
    return rows


def key_repartition(spark):
    processes = spark.read.parquet(str(PROCESSED / 'processes'))

    def two_aggregations(df):
        amounts = df.groupBy('buyer_id').agg(F.sum('award_amount_pen').alias('amount'))
        counts = df.groupBy('buyer_id').agg(F.count('*').alias('processes'))
        return amounts.join(counts, 'buyer_id')

    # Las variantes sin caché se miden antes de cachear: Spark sustituye automáticamente
    # cualquier subplan idéntico a uno cacheado, y repartition('buyer_id') lo sería.
    single_direct = processes.groupBy('buyer_id').count()
    single_repartitioned = processes.repartition('buyer_id').groupBy('buyer_id').count()
    direct = two_aggregations(processes)
    single = dict(
        direct=dict(exchanges=exchanges(single_direct), seconds=measure(single_direct.collect)),
        repartition_first=dict(exchanges=exchanges(single_repartitioned), seconds=measure(single_repartitioned.collect)))
    two = dict(direct=dict(exchanges=exchanges(direct), seconds=measure(direct.collect)))
    partitioned_input = processes.repartition('buyer_id').cache()
    partitioned_input.count()
    reused = two_aggregations(partitioned_input)
    two['repartitioned_cached'] = dict(exchanges=exchanges(reused), seconds=measure(reused.collect))
    partitioned_input.unpersist()
    return dict(single_aggregation=single, two_aggregations_and_join=two)


def run_experiment(spark, raw_paths, year):
    return dict(
        input=input_partitions(spark, raw_paths),
        pruning=pruning(spark, year),
        shuffle_partitions=shuffle_partitions(spark),
        key_repartition=key_repartition(spark),
        cluster=dict(default_parallelism=spark.sparkContext.defaultParallelism,
                     executor_cores=spark.conf.get('spark.executor.cores', None),
                     cores_max=spark.conf.get('spark.cores.max', None)),
    )
