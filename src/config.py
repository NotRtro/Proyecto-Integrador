import os
from pathlib import Path
RAW = Path(os.getenv('RAW_PATH', 'data/raw'))
PROCESSED = Path(os.getenv('PROCESSED_PATH', 'data/processed'))
ARTIFACTS = Path(os.getenv('ARTIFACTS_PATH', 'artifacts'))
YEARS = [int(x) for x in os.getenv('ANALYSIS_YEARS', '2023,2024,2025').split(',')]
SEED = int(os.getenv('RANDOM_SEED', '42'))
SAMPLE = int(os.getenv('SAMPLE_SIZE', '1500'))
APRIORI_SAMPLE = int(os.getenv('APRIORI_SAMPLE_SIZE', '10000'))


def spark_session():
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName('OECE Mineria').master(os.getenv('SPARK_MASTER_URL', 'local[2]'))
         .config('spark.sql.shuffle.partitions', '8').config('spark.default.parallelism', '8')
         .config('spark.executor.memory', os.getenv('SPARK_EXECUTOR_MEMORY', '2g'))
         .config('spark.cores.max', '4').config('spark.executor.cores', '2')
         .config('spark.sql.session.timeZone', 'America/Lima'))
    if os.getenv('SPARK_DRIVER_HOST'):
        b = b.config('spark.driver.host', os.environ['SPARK_DRIVER_HOST']).config('spark.driver.bindAddress', '0.0.0.0')
    s = b.getOrCreate()
    s.sparkContext.setLogLevel('WARN')
    return s
