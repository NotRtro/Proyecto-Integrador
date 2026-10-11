"""La misma agregación en Spark y en pandas, a distintos tamaños (Partes I y VI, G-04).

Motor: ambos, a propósito. Se mide el tiempo de punta a punta (leer el Parquet,
filtrar la fracción, agrupar por año y categoría y traer el resultado) y la memoria
que pandas necesita para tener la tabla en el driver.

Sirve para responder con datos "¿qué tareas se beneficiaron de Spark y cuáles eran
manejables en local?": con ~200 mil procesos, pandas en una sola máquina suele ganar
en una agregación simple; la ventaja de Spark aparece al parsear JSON anidado, al
escalar el volumen o cuando los datos no caben en la memoria de un solo proceso.
"""
import time

import pandas as pd
import pyarrow.dataset as ds
from pyspark.sql import functions as F

from src.config import PROCESSED, SEED

FRACTIONS = [0.1, 0.5, 1.0]
COLUMNS = ['ocid', 'year', 'category', 'tender_amount_pen']


def spark_aggregate(spark, fraction):
    df = spark.read.parquet(str(PROCESSED / 'processes')).select(*COLUMNS)
    if fraction < 1:
        df = df.sample(fraction=fraction, seed=SEED)
    return df.groupBy('year', 'category').agg(F.count('*').alias('processes'),
                                              F.sum('tender_amount_pen').alias('amount')).collect()


def pandas_aggregate(fraction):
    # pyarrow lee las particiones year=/month= como columnas, igual que Spark.
    table = ds.dataset(str(PROCESSED / 'processes'), format='parquet', partitioning='hive')
    df = table.to_table(columns=COLUMNS).to_pandas()
    if fraction < 1:
        df = df.sample(frac=fraction, random_state=SEED)
    result = df.groupby(['year', 'category'], dropna=False).agg(processes=('ocid', 'size'),
                                                                amount=('tender_amount_pen', 'sum'))
    return result, df.memory_usage(deep=True).sum()


def run_comparison(spark):
    rows = []
    for fraction in FRACTIONS:
        spark_aggregate(spark, fraction)  # calentamiento
        start = time.perf_counter()
        spark_rows = spark_aggregate(spark, fraction)
        spark_seconds = time.perf_counter() - start
        pandas_aggregate(fraction)
        start = time.perf_counter()
        pandas_result, memory = pandas_aggregate(fraction)
        pandas_seconds = time.perf_counter() - start
        rows.append(dict(fraction=fraction, processes=int(pandas_result.processes.sum()),
                         spark_seconds=spark_seconds, pandas_seconds=pandas_seconds,
                         pandas_memory_mb=memory / 1e6, groups_spark=len(spark_rows),
                         groups_pandas=len(pandas_result)))
    return pd.DataFrame(rows)
