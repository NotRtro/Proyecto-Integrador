import os
from pathlib import Path

RAW = Path(os.getenv('RAW_PATH', 'data/raw'))
PROCESSED = Path(os.getenv('PROCESSED_PATH', 'data/processed'))
ARTIFACTS = Path(os.getenv('ARTIFACTS_PATH', 'artifacts'))
YEARS = [int(x) for x in os.getenv('ANALYSIS_YEARS', '2023,2024,2025').split(',')]
SEED = int(os.getenv('RANDOM_SEED', '42'))
SAMPLE = int(os.getenv('SAMPLE_SIZE', '1500'))
APRIORI_SAMPLE = int(os.getenv('APRIORI_SAMPLE_SIZE', '10000'))

# Perfiles de ejecución (docs/DecisionesTecnicas.md). Lo que responde Q1-Q3 usa siempre
# todos los datos; el perfil solo cambia la evaluación de ANN (Parte III), que es una
# comparación de algoritmos y no necesita el corpus completo para sostener su conclusión.
PROFILES = {
    'completo': dict(
        ann_max_corpus=None,
        hnsw_m=[8, 16, 32],
        hnsw_ef_construction=[64, 128],
        hnsw_ef_search=[16, 32, 64, 128],
        ivf_nlist=[64, 256, 1024],
        ivf_nprobe=[1, 4, 16, 64],
    ),
    'rapido': dict(
        ann_max_corpus=50_000,
        hnsw_m=[16],
        hnsw_ef_construction=[128],
        hnsw_ef_search=[16, 32, 64, 128],
        ivf_nlist=[256],
        ivf_nprobe=[1, 4, 16, 64],
    ),
}
EXPERIMENT_MODES = ['si', 'no', 'auto']


def run_profile():
    """Perfil activo: 'completo' (por defecto, para el informe) o 'rapido' (corridas de trabajo)."""
    profile = os.getenv('RUN_PROFILE') or 'completo'
    if profile not in PROFILES:
        raise ValueError(f'RUN_PROFILE debe ser uno de {list(PROFILES)}')
    return profile


def profile_settings():
    return PROFILES[run_profile()]


def experiments_mode():
    """Mediciones de una vez: 'si' las repite, 'no' las omite, 'auto' solo si faltan.

    Por defecto el perfil completo las repite (corrida del informe) y el rápido las
    reutiliza si ya existen.
    """
    mode = os.getenv('RUN_EXPERIMENTS') or ('si' if run_profile() == 'completo' else 'auto')
    if mode not in EXPERIMENT_MODES:
        raise ValueError(f'RUN_EXPERIMENTS debe ser uno de {EXPERIMENT_MODES}')
    return mode


def refresh_requested():
    """--refresh: ignorar las cachés de validación y staging."""
    return os.getenv('RUN_REFRESH') == '1'


def spark_session():
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.appName('OECE Mineria')
        .master(os.getenv('SPARK_MASTER_URL', 'local[2]'))
        .config('spark.sql.shuffle.partitions', '8')
        .config('spark.default.parallelism', '8')
        .config('spark.executor.memory', os.getenv('SPARK_EXECUTOR_MEMORY', '2g'))
        .config('spark.cores.max', '4')
        .config('spark.executor.cores', '2')
        .config('spark.sql.session.timeZone', 'America/Lima')
    )
    if os.getenv('SPARK_DRIVER_HOST'):
        b = b.config('spark.driver.host', os.environ['SPARK_DRIVER_HOST']).config('spark.driver.bindAddress', '0.0.0.0')
    s = b.getOrCreate()
    s.sparkContext.setLogLevel('WARN')
    return s
