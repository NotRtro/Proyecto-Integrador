import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone
from src.config import ARTIFACTS, PROCESSED, SEED


def now():
    return datetime.now(timezone.utc).isoformat()


class StepTimer:
    """Mide la duración de subetapas y la guarda como artefacto.

    Spark es perezoso: solo se mide algo real si el bloque contiene una acción
    (write, count, collect). Los tiempos sirven para discutir qué partes se
    benefician de Spark (Parte VI).
    """

    def __init__(self):
        self.steps = []

    @contextmanager
    def step(self, name):
        start = time.perf_counter()
        yield
        seconds = time.perf_counter() - start
        self.steps.append(dict(step=name, seconds=round(seconds, 2)))
        print(f'  {name}: {seconds:.1f}s', flush=True)


def save_json(name, value):
    p = ARTIFACTS / name
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False))
    os.replace(tmp, p)


def save_csv(name, rows, columns=None):
    import pandas as pd
    p = ARTIFACTS / name
    p.parent.mkdir(parents=True, exist_ok=True)
    df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows, columns=columns)
    tmp = p.with_suffix('.tmp')
    df.to_csv(tmp, index=False)
    os.replace(tmp, p)


def processes(spark):
    """Tabla analítica completa de procesos (montos, conversiones, conteos)."""
    return spark.read.parquet(str(PROCESSED / 'processes'))


def features(spark):
    """Tabla delgada con texto, shingles y atributos categóricos (Partes II, III y V)."""
    return spark.read.parquet(str(PROCESSED / 'process_features'))


def sample_processes(spark, n):
    """Muestra reproducible: los n procesos con menor SHA-256(ocid + semilla)."""
    from pyspark.sql import functions as F
    return features(spark).orderBy(F.sha2(F.concat(F.col('ocid'), F.lit(str(SEED))), 256)).limit(n).toPandas()
