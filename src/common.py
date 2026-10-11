import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from src.config import ARTIFACTS, PROCESSED, SEED, experiments_mode, run_profile


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


EXPERIMENTS_REGISTRY = 'metadata/experiments.json'


def experiment(name, artifacts, measure):
    """Corre una medición de una vez (benchmark, escalamiento...) o reutiliza la guardada.

    Las mediciones no responden Q1-Q3: comparan motores o algoritmos. Repetirlas en cada
    corrida cuesta minutos sin cambiar la conclusión, así que según RUN_EXPERIMENTS
    se repiten ('si'), se omiten ('no') o se corren solo si faltan sus artefactos ('auto').
    El registro dice cuándo, con qué perfil y con qué versión del código se midió cada una.
    """
    path = ARTIFACTS / EXPERIMENTS_REGISTRY
    registry = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}
    mode = experiments_mode()
    available = all((ARTIFACTS / a).is_file() for a in artifacts)
    entry = registry.get(name, {})
    if mode == 'no' or (mode == 'auto' and available):
        entry.update(last_run=now(), last_status='reutilizado' if available else 'omitido: no hay medición previa')
        print(f'  experimento {name}: {entry["last_status"]}', flush=True)
        measured = False
    else:
        start = time.perf_counter()
        measure()
        entry = dict(
            measured_at=now(),
            seconds=round(time.perf_counter() - start, 1),
            profile=run_profile(),
            code_version=os.getenv('CODE_VERSION', 'uncommitted'),
            artifacts=artifacts,
            last_run=now(),
            last_status='medido',
        )

        measured = True
    registry[name] = entry
    save_json(EXPERIMENTS_REGISTRY, registry)
    return measured
