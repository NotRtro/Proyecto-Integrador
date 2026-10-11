"""Parte III: búsqueda aproximada de vecinos (HNSW e IVF) frente a búsqueda exacta.

Motor: local (scikit-learn, numpy y faiss en el driver). Spark solo lee la tabla de
features; ver vectors.py e indexes.py para la justificación.

Experimentos:
1. Escalamiento (III-02): corpus de 5 mil a ~196 mil procesos; para cada tamaño,
   latencia exacta frente a HNSW e IVF con parámetros fijos. Muestra a partir de qué
   tamaño la búsqueda aproximada empieza a compensar.
2. Rejillas sobre el corpus completo: HNSW (M, efConstruction, efSearch) e IVF
   (nlist, nprobe), con recall@5/@10 sobre 200 consultas e IC 95 % por bootstrap (III-04).
3. Memoria de la matriz exacta frente a cada índice (III-06).
4. Relación con la Parte II (III-05): de los pares casi idénticos que encontró LSH
   (Jaccard >= 0.7), ¿qué proporción aparece entre los 10 vecinos por coseno?

Artefactos (artifacts/ann/): scaling.csv, hnsw_grid.csv, ivf_grid.csv, neighbors_display.csv,
documents.csv, queries.json, methodology.json.
"""

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from src.common import StepTimer, save_csv, save_json
from src.common import features as read_features
from src.config import ARTIFACTS, SEED, spark_session
from src.part3.indexes import ann_search, bootstrap_ci, build_hnsw, build_ivf, exact_search, index_bytes, recall_at
from src.part3.vectors import build_vectors

K = 10
QUERIES = 200
DISPLAY_QUERIES = 40
SCALING_SIZES = [5_000, 20_000, 50_000, 100_000]
SCALING_HNSW = dict(m=16, ef_construction=128, ef_search=64)
SCALING_NPROBE = 16
HNSW_M = [8, 16, 32]
HNSW_EF_CONSTRUCTION = [64, 128]
HNSW_EF_SEARCH = [16, 32, 64, 128]
IVF_NLIST = [64, 256, 1024]
IVF_NPROBE = [1, 4, 16, 64]
TARGET_RECALL = 0.95
PART2_PAIRS = 300


def latency_stats(latencies, exact_mean):
    return dict(
        mean_ms=1000 * latencies.mean(),
        median_ms=1000 * np.median(latencies),
        p95_ms=1000 * np.percentile(latencies, 95),
        speedup=exact_mean / latencies.mean(),
    )


def recall_stats(found, exact, seed):
    stats = {}
    for k in (5, 10):
        values = recall_at(found, exact, k)
        low, high = bootstrap_ci(values, seed)
        stats.update(
            {f'recall_at_{k}': float(values.mean()), f'recall_at_{k}_ci_low': low, f'recall_at_{k}_ci_high': high}
        )
    return stats


def ivf_nlist_for(size):
    """Regla usual: nlist ≈ 4·sqrt(N), redondeado a potencia de 2."""
    return int(2 ** round(np.log2(4 * np.sqrt(size))))


def scaling(vectors, queries):
    rows = []
    for size in [s for s in SCALING_SIZES if s < len(vectors)] + [len(vectors)]:
        subset = vectors[:size]
        exact, exact_lat = exact_search(subset, queries, K)
        hnsw, hnsw_build = build_hnsw(subset, SCALING_HNSW['m'], SCALING_HNSW['ef_construction'])
        hnsw.hnsw.efSearch = SCALING_HNSW['ef_search']
        hnsw_found, hnsw_lat = ann_search(hnsw, subset, queries, K)
        nlist = ivf_nlist_for(size)
        ivf, ivf_build = build_ivf(subset, nlist, SEED)
        ivf.nprobe = SCALING_NPROBE
        ivf_found, ivf_lat = ann_search(ivf, subset, queries, K)
        rows.append(
            dict(
                corpus_size=size,
                exact_mean_ms=1000 * exact_lat.mean(),
                hnsw_mean_ms=1000 * hnsw_lat.mean(),
                hnsw_build_seconds=hnsw_build,
                hnsw_recall_at_10=float(recall_at(hnsw_found, exact, K).mean()),
                ivf_nlist=nlist,
                ivf_mean_ms=1000 * ivf_lat.mean(),
                ivf_build_seconds=ivf_build,
                ivf_recall_at_10=float(recall_at(ivf_found, exact, K).mean()),
                exact_matrix_mb=subset.nbytes / 1e6,
            )
        )
        print(
            f'ANN escalamiento N={size}: exacto {rows[-1]["exact_mean_ms"]:.2f} ms, '
            f'HNSW {rows[-1]["hnsw_mean_ms"]:.3f} ms',
            flush=True,
        )
    return pd.DataFrame(rows)


def hnsw_grid(vectors, queries, exact, exact_mean):
    rows, found_by_config = [], {}
    for m in HNSW_M:
        for ef_construction in HNSW_EF_CONSTRUCTION:
            index, build = build_hnsw(vectors, m, ef_construction)
            size = index_bytes(index)
            for ef_search in HNSW_EF_SEARCH:
                index.hnsw.efSearch = ef_search
                found, latencies = ann_search(index, vectors, queries, K)
                config = f'M{m}_efC{ef_construction}_efS{ef_search}'
                found_by_config[config] = found
                rows.append(
                    dict(
                        config=config,
                        M=m,
                        efConstruction=ef_construction,
                        efSearch=ef_search,
                        **recall_stats(found, exact, SEED),
                        **latency_stats(latencies, exact_mean),
                        exact_mean_ms=1000 * exact_mean,
                        build_seconds=build,
                        index_bytes=size,
                        corpus_size=len(vectors),
                        queries=len(queries),
                        kmax=K,
                    )
                )
            print(f'HNSW M={m} efC={ef_construction}: construcción {build:.1f}s', flush=True)
    return pd.DataFrame(rows), found_by_config


def ivf_grid(vectors, queries, exact, exact_mean):
    rows, found_by_config = [], {}
    for nlist in IVF_NLIST:
        index, build = build_ivf(vectors, nlist, SEED)
        size = index_bytes(index)
        for nprobe in IVF_NPROBE:
            index.nprobe = nprobe
            found, latencies = ann_search(index, vectors, queries, K)
            config = f'IVF{nlist}_nprobe{nprobe}'
            found_by_config[config] = found
            rows.append(
                dict(
                    config=config,
                    nlist=nlist,
                    nprobe=nprobe,
                    cells_scanned=nprobe / nlist,
                    **recall_stats(found, exact, SEED),
                    **latency_stats(latencies, exact_mean),
                    exact_mean_ms=1000 * exact_mean,
                    build_seconds=build,
                    index_bytes=size,
                    corpus_size=len(vectors),
                    queries=len(queries),
                    kmax=K,
                )
            )
    return pd.DataFrame(rows), found_by_config


def best(grid):
    """La configuración más rápida que alcanza el recall objetivo (o la de mayor recall)."""
    eligible = grid[grid.recall_at_10 >= TARGET_RECALL]
    return (
        eligible.sort_values('mean_ms') if not eligible.empty else grid.sort_values('recall_at_10', ascending=False)
    ).iloc[0]


def part2_agreement(vectors, position):
    """Proporción de pares LSH (Jaccard >= 0.7) cuyo par está en el top-10 coseno."""
    path = ARTIFACTS / 'similarity' / 'repeated_requirements.parquet'
    if not path.is_file():
        return None
    pairs = pd.read_parquet(path, columns=['left', 'right', 'jaccard'])
    pairs = pairs[pairs.left.isin(position) & pairs.right.isin(position)]
    pairs = pairs.sample(min(PART2_PAIRS, len(pairs)), random_state=SEED)
    if pairs.empty:
        return None
    left = np.array([position[o] for o in pairs.left])
    right = np.array([position[o] for o in pairs.right])
    neighbors, _ = exact_search(vectors, left, K)
    # Con grupos de cientos de textos idénticos, el par está empatado con muchos vecinos y
    # puede quedar fuera de un top-10 literal. Cuenta como acierto si su similitud alcanza
    # la del décimo vecino (está en el top-10 salvo empates).
    pair_similarity = np.einsum('ij,ij->i', vectors[left], vectors[right])
    tenth = np.einsum('ij,ij->i', vectors[left], vectors[neighbors[:, -1]])
    literal = np.array([r in n for r, n in zip(right, neighbors)])
    return dict(
        pairs=len(pairs),
        in_top10=float(np.mean(pair_similarity >= tenth - 1e-6)),
        in_top10_literal=float(literal.mean()),
        mean_jaccard=float(pairs.jaccard.mean()),
        mean_cosine=float(pair_similarity.mean()),
    )


def display(data, vectors, queries, exact, configs):
    rows = []
    for config, found in configs.items():
        for q, neighbors in zip(queries, found):
            for rank, j in enumerate(neighbors[:K], 1):
                if j >= 0:
                    rows.append(
                        dict(
                            config=config,
                            query_ocid=data.ocid[q],
                            neighbor_ocid=data.ocid[j],
                            rank=rank,
                            similarity=float(vectors[q] @ vectors[j]),
                        )
                    )
    used = {r['query_ocid'] for r in rows} | {r['neighbor_ocid'] for r in rows}
    return pd.DataFrame(rows), data[data.ocid.isin(used)][['ocid', 'text', 'year', 'category']]


def run(spark):
    timer = StepTimer()
    with timer.step('lectura y vectorización TF-IDF + SVD'):
        # Orden pseudoaleatorio reproducible: los prefijos del arreglo son submuestras aleatorias.
        data = (
            read_features(spark)
            .filter(F.length('normalized_text') > 0)
            .select('ocid', 'normalized_text', 'text', 'year', 'category')
            .orderBy(F.sha2(F.concat(F.col('ocid'), F.lit(str(SEED))), 256))
            .toPandas()
        )
        vectors, info = build_vectors(data.normalized_text)
        keep = np.linalg.norm(vectors, axis=1) > 0
        vectors, data = vectors[keep], data[keep].reset_index(drop=True)
    rng = np.random.default_rng(SEED)
    # Consultas del primer bloque de 5 mil: están en todos los tamaños del escalamiento.
    queries = rng.choice(min(SCALING_SIZES[0], len(vectors)), size=min(QUERIES, len(vectors) - 1), replace=False)

    with timer.step('escalamiento'):
        save_csv('ann/scaling.csv', scaling(vectors, queries))
    with timer.step('búsqueda exacta (referencia)'):
        exact, exact_latencies = exact_search(vectors, queries, K)
    with timer.step('rejilla HNSW'):
        hnsw, hnsw_found = hnsw_grid(vectors, queries, exact, exact_latencies.mean())
        save_csv('ann/hnsw_grid.csv', hnsw)
    with timer.step('rejilla IVF'):
        ivf, ivf_found = ivf_grid(vectors, queries, exact, exact_latencies.mean())
        save_csv('ann/ivf_grid.csv', ivf)

    best_hnsw, best_ivf = best(hnsw), best(ivf)
    shown = queries[:DISPLAY_QUERIES]
    configs = {
        'exact': exact[:DISPLAY_QUERIES],
        best_hnsw.config: hnsw_found[best_hnsw.config][:DISPLAY_QUERIES],
        best_ivf.config: ivf_found[best_ivf.config][:DISPLAY_QUERIES],
    }
    neighbors, documents = display(data, vectors, shown, exact, configs)
    save_csv('ann/neighbors_display.csv', neighbors)
    save_csv('ann/documents.csv', documents)
    save_json('ann/queries.json', [dict(ocid=data.ocid[q], text=data.text[q]) for q in shown])
    agreement = part2_agreement(vectors, {o: i for i, o in enumerate(data.ocid)})
    save_json(
        'ann/methodology.json',
        dict(
            vectors=info,
            queries=len(queries),
            k=K,
            target_recall=TARGET_RECALL,
            exact=dict(mean_ms=1000 * float(exact_latencies.mean()), matrix_mb=vectors.nbytes / 1e6),
            best_hnsw=best_hnsw.to_dict(),
            best_ivf=best_ivf.to_dict(),
            part2_agreement=agreement,
            latency='Una consulta a la vez, un hilo, sin la propia consulta entre los vecinos.',
            recall='Recall@k = fracción de los k vecinos exactos recuperados; IC 95 % por bootstrap sobre las '
            'consultas.',
        ),
    )
    # Compatibilidad con la API actual (paso 7.2 la reemplaza por ivf_grid.csv).
    save_json(
        'ann/ivf_status.json',
        dict(implemented=True, reason='IVF implementado con faiss IndexIVFFlat: ver ivf_grid.csv.'),
    )
    save_json('ann/part3_timings.json', timer.steps)


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
