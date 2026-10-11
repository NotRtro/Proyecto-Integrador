"""Búsqueda exacta, HNSW e IVF sobre los mismos vectores (Parte III).

Motor: local (numpy y faiss). Un índice ANN vive en la memoria de una máquina y
responde consultas individuales en microsegundos; repartirlo en Spark agregaría
latencia de red sin beneficio a este volumen.

Cómo funcionan (resumen; la web lo explica con diagramas):
- Exacta (fuerza bruta): producto interno de la consulta contra todos los vectores y
  los k mayores. Siempre correcta; costo O(N·d) por consulta.
- HNSW (Hierarchical Navigable Small World): grafo por capas donde cada vector se une a
  sus M vecinos más cercanos. La búsqueda baja desde la capa superior (pocos nodos,
  saltos largos) hasta la inferior (todos los nodos, saltos cortos). efConstruction
  controla la calidad del grafo al construirlo; efSearch, cuántos candidatos se
  exploran por consulta (más ef = más recall y más tiempo).
- IVF (Inverted File): k-means divide el espacio en nlist celdas; cada vector se guarda
  en la lista de su centroide. Una consulta solo revisa las nprobe celdas más cercanas
  (más nprobe = más recall y más tiempo; nprobe = nlist equivale a fuerza bruta).

La latencia se mide consulta por consulta en un solo hilo, para que las tres
estrategias compitan en igualdad. La construcción usa varios hilos.
"""

import time

import faiss
import numpy as np
from threadpoolctl import threadpool_limits

BUILD_THREADS = 4


def exact_search(vectors, queries, k):
    """Vecinos exactos (sin la propia consulta) y latencia media por consulta."""
    neighbors, latencies = [], []
    with threadpool_limits(limits=1):
        for q in queries:
            start = time.perf_counter()
            scores = vectors @ vectors[q]
            scores[q] = -np.inf
            top = np.argpartition(-scores, k)[:k]
            neighbors.append(top[np.argsort(-scores[top])])
            latencies.append(time.perf_counter() - start)
    return np.array(neighbors), np.array(latencies)


def build_hnsw(vectors, m, ef_construction):
    faiss.omp_set_num_threads(BUILD_THREADS)
    index = faiss.IndexHNSWFlat(vectors.shape[1], m, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = ef_construction
    start = time.perf_counter()
    index.add(vectors)
    return index, time.perf_counter() - start


def build_ivf(vectors, nlist, seed):
    faiss.omp_set_num_threads(BUILD_THREADS)
    quantizer = faiss.IndexFlatIP(vectors.shape[1])
    index = faiss.IndexIVFFlat(quantizer, vectors.shape[1], nlist, faiss.METRIC_INNER_PRODUCT)
    index.cp.seed = seed
    start = time.perf_counter()
    index.train(vectors)
    index.add(vectors)
    return index, time.perf_counter() - start


def ann_search(index, vectors, queries, k):
    """Vecinos aproximados (sin la propia consulta) y latencia por consulta, en un hilo."""
    faiss.omp_set_num_threads(1)
    neighbors, latencies = [], []
    for q in queries:
        start = time.perf_counter()
        _, labels = index.search(vectors[q : q + 1], k + 1)
        latencies.append(time.perf_counter() - start)
        found = [int(j) for j in labels[0] if j != q and j >= 0][:k]
        neighbors.append(found + [-1] * (k - len(found)))
    return np.array(neighbors), np.array(latencies)


def index_bytes(index):
    return int(faiss.serialize_index(index).nbytes)


def recall_at(approx, exact, k):
    """Recall@k por consulta: fracción de los k vecinos exactos que la búsqueda aproximada recuperó."""
    return np.array([len(set(a[:k]) & set(e[:k])) / k for a, e in zip(approx, exact)])


def bootstrap_ci(values, seed, rounds=2000):
    """Intervalo de confianza 95 % de la media por bootstrap."""
    rng = np.random.default_rng(seed)
    means = rng.choice(values, size=(rounds, len(values)), replace=True).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))
