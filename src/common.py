import json
import os
from pathlib import Path
from datetime import datetime, timezone
from src.config import ARTIFACTS, PROCESSED, SEED


def now():
    return datetime.now(timezone.utc).isoformat()


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
    return spark.read.parquet(str(PROCESSED / 'process_features'))


def sample_processes(spark, n):
    from pyspark.sql import functions as F
    return processes(spark).orderBy(F.sha2(F.concat(F.col('ocid'), F.lit(str(SEED))), 256)).limit(n).toPandas()
