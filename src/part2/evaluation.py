"""Evaluación de MinHash y LSH contra el Jaccard exacto, sobre una muestra (Parte II).

Motor: mixto, por diseño.
- La verdad exacta (todos los pares de la muestra) se calcula en el driver con una
  matriz dispersa documentos x shingles: X·Xᵀ da las intersecciones de todos los pares
  a la vez. Comparar todos contra todos es justo lo que no escala, por eso solo se
  hace sobre la muestra.
- Las firmas y los candidatos LSH de la rejilla se calculan con RDDs de Spark.

Muestra por bloques: elegir procesos al azar casi no deja pares similares (dos compras
al azar rara vez se parecen) y con tan pocos positivos la precisión y el recall son
ruido. Se toman entidades completas, elegidas por hash, porque una misma entidad
repite requerimientos; los pares entre entidades distintas siguen dando negativos.
"""
import itertools
import time

import numpy as np
from pyspark.sql import functions as F
from scipy import sparse

from src.config import SEED
from src.part2.minhash_lsh import band_keys, candidate_probability, coefficients, effective_threshold, signature

MAX_HASHES = 256
ERROR_HASHES = [16, 32, 64, 128, 256]
GRID_HASHES = [32, 64, 128]
GRID_ROWS = [2, 4, 8]
THRESHOLDS = [0.5, 0.6, 0.7, 0.8]
ERROR_SUBSET = 0.2          # el error se reporta también solo para pares con Jaccard >= 0.2
MAX_BUYER_SHARE = 0.2       # ninguna entidad aporta más del 20 % de la muestra
EXPORT_PAIRS = 300          # pares por (configuración, estado) para la web


def block_sample(features, size):
    """Procesos de entidades completas elegidas por hash, hasta `size` documentos."""
    with_text = features.filter(F.size('shingles') > 0)
    buyers = (with_text.groupBy('buyer_id').count()
              .withColumn('h', F.sha2(F.concat(F.col('buyer_id'), F.lit(str(SEED))), 256))
              .orderBy('h').collect())
    chosen, total = [], 0
    for row in buyers:
        if row['count'] > size * MAX_BUYER_SHARE:
            continue
        chosen.append(row['buyer_id'])
        total += row['count']
        if total >= size:
            break
    sample = (with_text.filter(F.col('buyer_id').isin(chosen))
              .orderBy(F.sha2(F.concat(F.col('ocid'), F.lit(str(SEED))), 256)).limit(size)
              .toPandas())
    return sample.reset_index(drop=True), len(chosen)


def exact_jaccard(shingle_sets):
    """Jaccard exacto de todos los pares i < j, vía producto de matriz dispersa.

    Devuelve (i, j, jaccard) solo para pares con al menos un shingle en común; el resto
    tiene Jaccard 0.
    """
    vocabulary = {}
    rows, cols = [], []
    for doc, shingles in enumerate(shingle_sets):
        for token in set(shingles):
            rows.append(doc)
            cols.append(vocabulary.setdefault(token, len(vocabulary)))
    x = sparse.csr_matrix((np.ones(len(rows), dtype=np.int32), (rows, cols)), shape=(len(shingle_sets), len(vocabulary)))
    sizes = np.asarray(x.sum(axis=1)).ravel()
    intersections = sparse.triu(x @ x.T, k=1).tocoo()
    union = sizes[intersections.row] + sizes[intersections.col] - intersections.data
    return intersections.row, intersections.col, intersections.data / union


def dense_pair_index(n):
    """Índices (i, j) de todos los pares i < j en el orden de np.triu_indices."""
    return np.triu_indices(n, k=1)


def minhash_error(signatures, truth_i, truth_j, truth_values):
    """Error del Jaccard estimado frente al exacto para cada número de funciones hash."""
    n = len(signatures)
    exact = np.zeros(n * (n - 1) // 2)
    position = truth_i * n - truth_i * (truth_i + 1) // 2 + (truth_j - truth_i - 1)
    exact[position] = truth_values
    estimates = {k: np.empty_like(exact) for k in ERROR_HASHES}
    offset = 0
    for i in range(n - 1):
        equal = signatures[i + 1:] == signatures[i]        # (n-i-1) x MAX_HASHES
        cumulative = np.cumsum(equal, axis=1)
        for k in ERROR_HASHES:
            estimates[k][offset:offset + n - i - 1] = cumulative[:, k - 1] / k
        offset += n - i - 1
    rows, scatter = [], []
    similar = exact >= ERROR_SUBSET
    rng = np.random.default_rng(SEED)
    shown = np.concatenate([rng.choice(np.flatnonzero(similar), min(600, similar.sum()), replace=False),
                            rng.choice(np.flatnonzero(~similar), min(200, (~similar).sum()), replace=False)])
    for k, estimate in estimates.items():
        error = estimate - exact
        theoretical = np.sqrt(np.mean(exact[similar] * (1 - exact[similar]) / k)) if similar.any() else None
        rows.append(dict(hashes=k, pairs=len(exact), mae=np.abs(error).mean(), rmse=np.sqrt((error ** 2).mean()),
                         bias=error.mean(), similar_pairs=int(similar.sum()),
                         mae_similar=np.abs(error[similar]).mean() if similar.any() else None,
                         rmse_similar=np.sqrt((error[similar] ** 2).mean()) if similar.any() else None,
                         rmse_theoretical_similar=theoretical))
        scatter.extend(dict(hashes=k, exact=float(exact[p]), estimate=float(estimate[p])) for p in shown)
    return rows, scatter, exact


def lsh_candidates(spark, signatures, bands, rows):
    """Pares candidatos (i < j) con bandas LSH en Spark (RDD: banda -> documentos)."""
    indexed = spark.sparkContext.parallelize(list(enumerate(signatures)))
    return set(indexed
               .flatMap(lambda item: [(key, item[0]) for key in band_keys(item[1], bands, rows)])
               .groupByKey()
               .flatMap(lambda kv: itertools.combinations(sorted(set(kv[1])), 2))
               .distinct()
               .collect())


def lsh_grid(spark, signatures, exact, n):
    """Rejilla (k, r) x umbral: precisión, recall, F1, TP/FP/FN, tiempos y curva S observada."""
    pair_i, pair_j = dense_pair_index(n)
    grid, curve, pairs = [], [], []
    bins = np.linspace(0, 1, 11)
    exact_bin = np.clip(np.digitize(exact, bins) - 1, 0, 9)
    for hashes in GRID_HASHES:
        for rows in GRID_ROWS:
            bands = hashes // rows
            start = time.perf_counter()
            candidates = lsh_candidates(spark, signatures[:, :hashes], bands, rows)
            lsh_seconds = time.perf_counter() - start
            is_candidate = np.zeros(len(exact), dtype=bool)
            if candidates:
                ci = np.array([c[0] for c in candidates])
                cj = np.array([c[1] for c in candidates])
                is_candidate[ci * n - ci * (ci + 1) // 2 + (cj - ci - 1)] = True
            for b in range(10):
                in_bin = exact_bin == b
                if in_bin.any():
                    middle = (bins[b] + bins[b + 1]) / 2
                    curve.append(dict(hashes=hashes, bands=bands, rows=rows, similarity=middle, pairs=int(in_bin.sum()),
                                      observed=float(is_candidate[in_bin].mean()),
                                      theoretical=float(candidate_probability(middle, bands, rows))))
            for threshold in THRESHOLDS:
                truth = exact >= threshold
                tp = int((is_candidate & truth).sum())
                fp = int((is_candidate & ~truth).sum())
                fn = int((~is_candidate & truth).sum())
                precision = tp / (tp + fp) if tp + fp else 0.0
                recall = tp / (tp + fn) if tp + fn else 0.0
                config = f'h{hashes}_b{bands}_r{rows}_t{threshold}'
                grid.append(dict(config=config, hashes=hashes, bands=bands, rows=rows, threshold=threshold,
                                 effective_threshold=effective_threshold(bands, rows), precision=precision,
                                 recall=recall, f1=2 * precision * recall / (precision + recall) if precision + recall else 0.0,
                                 tp=tp, fp=fp, fn=fn, candidates=int(is_candidate.sum()), truth_pairs=int(truth.sum()),
                                 all_pairs=len(exact), lsh_seconds=lsh_seconds))
                for status, mask in [('TP', is_candidate & truth), ('FP', is_candidate & ~truth), ('FN', ~is_candidate & truth)]:
                    chosen = np.flatnonzero(mask)
                    chosen = chosen[np.argsort(-exact[chosen])][:EXPORT_PAIRS]
                    pairs.extend(dict(config=config, i=int(pair_i[p]), j=int(pair_j[p]), jaccard=float(exact[p]), status=status)
                                 for p in chosen)
    return grid, curve, pairs


def evaluate(spark, features, size):
    """Corre toda la evaluación sobre la muestra por bloques."""
    data, buyers = block_sample(features, size)
    shingle_sets = [list(s) for s in data.shingles]
    timings = {}
    start = time.perf_counter()
    truth_i, truth_j, truth_values = exact_jaccard(shingle_sets)
    timings['exact_jaccard_seconds'] = time.perf_counter() - start
    start = time.perf_counter()
    coeffs = coefficients(MAX_HASHES, SEED)
    signatures = np.vstack([signature(s, coeffs=coeffs) for s in shingle_sets])
    timings['signature_seconds'] = time.perf_counter() - start
    error, scatter, exact = minhash_error(signatures, truth_i, truth_j, truth_values)
    grid, curve, pairs = lsh_grid(spark, signatures, exact, len(data))
    for row in grid:
        row.update(timings)
    for pair in pairs:
        left, right = data.iloc[pair.pop('i')], data.iloc[pair.pop('j')]
        pair.update(left_ocid=left.ocid, right_ocid=right.ocid, left_year=int(left.year), right_year=int(right.year),
                    left_category=left.category, right_category=right.category,
                    same_buyer=bool(left.buyer_id == right.buyer_id))
    return dict(data=data, buyers=buyers, error=error, scatter=scatter, grid=grid, curve=curve, pairs=pairs,
                timings=timings)
