import json
import os
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(os.getenv("ARTIFACTS_PATH", "artifacts"))


@lru_cache(maxsize=64)
def _frame(name: str, stamp: int) -> pd.DataFrame:
    path = ROOT / name
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def frame(name: str) -> pd.DataFrame:
    path = ROOT / name
    if not path.is_file():
        raise FileNotFoundError(name)
    return _frame(name, path.stat().st_mtime_ns)


@lru_cache(maxsize=64)
def _document(name: str, stamp: int):
    path = ROOT / name
    return json.loads(path.read_text(encoding="utf-8"))


def document(name: str):
    path = ROOT / name
    if not path.is_file():
        raise FileNotFoundError(name)
    return _document(name, path.stat().st_mtime_ns)


def records(data: pd.DataFrame) -> list[dict]:
    clean = data.replace({np.nan: None, np.inf: None, -np.inf: None})
    return clean.to_dict(orient="records")


def apply_filters(data: pd.DataFrame, filters: dict) -> pd.DataFrame:
    result = data
    for column, value in filters.items():
        if value is not None and value != "Todos" and column in result.columns:
            result = result[result[column].astype(str) == str(value)]
    return result


def options(data: pd.DataFrame, columns: list[str]) -> dict[str, list]:
    result = {}
    for column in columns:
        values = data[column].dropna().unique().tolist()
        result[column] = sorted(values, key=lambda value: str(value))
    return result
