import numpy as np
import pytest

faiss = pytest.importorskip('faiss')

from src.part3.indexes import ann_search, bootstrap_ci, build_hnsw, build_ivf, exact_search, recall_at  # noqa: E402


@pytest.fixture(scope='module')
def vectors():
    rng = np.random.default_rng(0)
    centers = rng.normal(size=(20, 32))
    points = np.repeat(centers, 100, axis=0) + 0.3 * rng.normal(size=(2000, 32))
    points /= np.linalg.norm(points, axis=1, keepdims=True)
    return np.ascontiguousarray(points, dtype=np.float32)


def test_exact_search_excludes_query_and_orders_by_similarity(vectors):
    neighbors, latencies = exact_search(vectors, np.array([0, 5]), 10)
    assert neighbors.shape == (2, 10) and 0 not in neighbors[0] and len(latencies) == 2
    scores = vectors[neighbors[0]] @ vectors[0]
    assert np.all(np.diff(scores) <= 1e-6)


def test_hnsw_and_ivf_recall(vectors):
    queries = np.arange(0, 2000, 40)
    exact, _ = exact_search(vectors, queries, 10)
    hnsw, _ = build_hnsw(vectors, 16, 64)
    hnsw.hnsw.efSearch = 64
    found, _ = ann_search(hnsw, vectors, queries, 10)
    assert recall_at(found, exact, 10).mean() > 0.9
    ivf, _ = build_ivf(vectors, 16, seed=1)
    ivf.nprobe = 16  # revisar todas las celdas equivale a fuerza bruta
    found, _ = ann_search(ivf, vectors, queries, 10)
    assert recall_at(found, exact, 10).mean() == pytest.approx(1.0)


def test_bootstrap_ci_contains_mean():
    values = np.array([1.0, 0.8, 0.9, 1.0, 0.7, 1.0])
    low, high = bootstrap_ci(values, seed=0)
    assert low <= values.mean() <= high
