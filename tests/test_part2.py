import random

import numpy as np
import pytest

from src.part2.evaluation import exact_jaccard, minhash_error
from src.part2.exact_jaccard import jaccard
from src.part2.minhash_lsh import (
    MERSENNE,
    band_keys,
    candidate_probability,
    coefficients,
    effective_threshold,
    estimate,
    signature,
)


def test_signature_is_order_invariant_and_seeded():
    assert np.array_equal(signature(['a', 'b']), signature(['b', 'a', 'a']))
    assert not np.array_equal(signature(['a'], 32, 1), signature(['a'], 32, 2))
    assert (signature([], 16) == MERSENNE).all()


def test_minhash_estimate_error_matches_theory():
    # A y B con 60 elementos en común sobre 100 -> J = 0.6; con k = 256 el error estándar es ~0.03.
    rng = random.Random(1)
    errors = []
    for trial in range(30):
        common = [f'c{trial}_{i}' for i in range(60)]
        a = common + [f'a{trial}_{i}' for i in range(20)]
        b = common + [f'b{trial}_{i}' for i in range(20)]
        coeffs = coefficients(256, rng.randrange(10**6))
        errors.append(estimate(signature(a, coeffs=coeffs), signature(b, coeffs=coeffs)) - jaccard(a, b))
    assert abs(np.mean(errors)) < 0.02  # insesgado
    assert np.sqrt(np.mean(np.square(errors))) < 0.06  # cerca de sqrt(0.6*0.4/256) = 0.031


def test_sparse_exact_jaccard_matches_definition():
    rng = random.Random(7)
    docs = [[f't{rng.randrange(15)}' for _ in range(rng.randrange(1, 8))] for _ in range(25)]
    rows, cols, values = exact_jaccard(docs)
    computed = {(int(i), int(j)): v for i, j, v in zip(rows, cols, values)}
    for i in range(len(docs)):
        for j in range(i + 1, len(docs)):
            assert computed.get((i, j), 0.0) == pytest.approx(jaccard(docs[i], docs[j]))


def test_minhash_error_reports_all_pairs():
    docs = [['a b', 'b c', 'c d'], ['a b', 'b c', 'x y'], ['q r']]
    i, j, v = exact_jaccard(docs)
    coeffs = coefficients(256, 3)
    sigs = np.vstack([signature(d, coeffs=coeffs) for d in docs])
    rows, _, exact = minhash_error(sigs, i, j, v)
    assert len(exact) == 3 and exact[0] == pytest.approx(0.5) and exact[1] == 0 and exact[2] == 0
    assert all(row['pairs'] == 3 for row in rows)


def test_band_keys_and_s_curve():
    sig = np.arange(16, dtype=np.uint64)
    keys = band_keys(sig, 4, 4)
    assert len(keys) == 4 and keys[0][0] == 0 and keys[0] != keys[1]
    assert band_keys(sig.copy(), 4, 4) == keys
    assert candidate_probability(1.0, 16, 8) == pytest.approx(1.0)
    assert candidate_probability(0.0, 16, 8) == pytest.approx(0.0)
    assert effective_threshold(16, 8) == pytest.approx(0.707, abs=0.01)


@pytest.mark.integration
def test_full_corpus_lsh_finds_near_duplicates():
    from src.config import spark_session
    from src.part2 import full_corpus

    spark = spark_session()
    try:
        base = [f'w{i} w{i + 1}' for i in range(30)]
        rows = [('d1', base), ('d2', base[:29] + ['zz yy']), ('d3', [f'k{i} k{i + 1}' for i in range(30)])]
        features = spark.createDataFrame(rows, 'ocid string, shingles array<string>')
        features = features.selectExpr('*', "'b' AS buyer_id", '2024 AS year', "'t' AS text")
        summary, similar, _, cached = full_corpus.run_full_corpus(spark, features)
        pairs = {(r.left, r.right): r.jaccard for r in similar.collect()}
        # d1 y d2 comparten 29 de 31 shingles (J ≈ 0.94); d3 no comparte nada.
        assert ('d1', 'd2') in pairs and pairs[('d1', 'd2')] == pytest.approx(29 / 31)
        assert summary['documents'] == 3 and summary['possible_pairs'] == 3
        for df in cached:
            df.unpersist()
    finally:
        spark.stop()


@pytest.mark.integration
def test_repeated_requirements_classification():
    from src.config import spark_session
    from src.part2 import repeated

    spark = spark_session()
    try:
        cols = (
            'ocid string, buyer_id string, buyer_name string, buyer_level string, department string, category string, '
            'method string, effective_date string, year int, tender_code string, tender_amount_pen double'
        )
        processes = spark.createDataFrame(
            [
                ('p1', 'b1', 'B1', 'local', 'LIMA', 'goods', 'AS', '2024-01-01', 2024, 'AS-SM-1-2024-X-1', 10.0),
                ('p2', 'b1', 'B1', 'local', 'LIMA', 'goods', 'AS', '2024-01-10', 2024, 'AS-SM-1-2024-X-2', 10.0),
                ('p3', 'b1', 'B1', 'local', 'LIMA', 'goods', 'AS', '2024-06-01', 2024, 'AS-SM-9-2024-X-1', 10.0),
                ('p4', 'b2', 'B2', 'local', 'PIURA', 'goods', 'AS', '2024-01-05', 2024, 'AS-SM-3-2024-Y-1', 10.0),
                # Mismo procedimiento que p1: la nomenclatura solo cambia en la puntuación.
                # (republicado días después: la nomenclatura identifica el procedimiento, no la fecha).
                ('p5', 'b1', 'B1', 'local', 'LIMA', 'goods', 'AS', '2024-01-04', 2024, 'AS-SM-1-2024-X/1', 10.0),
            ],
            cols,
        )
        similar = spark.createDataFrame(
            [('p1', 'p2', 0.9), ('p1', 'p3', 0.8), ('p1', 'p4', 0.75), ('p2', 'p3', 0.8), ('p1', 'p5', 1.0)],
            'left string, right string, jaccard double',
        )
        pairs = {(r.left, r.right): r for r in repeated.classify(similar, processes).collect()}
        assert pairs[('p1', 'p2')].kind == repeated.CATEGORIES[0] and pairs[('p1', 'p2')].reconvocation
        assert pairs[('p1', 'p3')].kind == repeated.CATEGORIES[1] and not pairs[('p1', 'p3')].reconvocation
        assert pairs[('p1', 'p4')].kind == repeated.CATEGORIES[2]
        assert pairs[('p1', 'p5')].same_procedure and not pairs[('p1', 'p5')].reconvocation
        assert not any(pairs[k].same_procedure for k in [('p1', 'p2'), ('p1', 'p3'), ('p1', 'p4')])
        involved = {
            r.kind: r.processes_involved for r in repeated.summary(repeated.classify(similar, processes)).collect()
        }
        assert involved[repeated.CATEGORIES[1]] == 3  # pares (p1,p3) y (p2,p3): tres procesos distintos
        top = repeated.top_buyers(repeated.classify(similar, processes)).collect()
        assert top == []  # en <= 30 días solo hay una reconvocatoria y un par del mismo procedimiento
    finally:
        spark.stop()


def test_groups_components_and_label():
    from src.part2 import groups

    found = groups.components([('a', 'b'), ('b', 'c'), ('x', 'y')])
    assert found['a'] == found['b'] == found['c'] and found['x'] == found['y'] and found['a'] != found['x']
    texts = ['tercero supervisor ambiental oefa', 'tercero supervisor ambiental campo', 'tercero supervisor ambiental']
    df = {'tercero': 10, 'supervisor': 50, 'ambiental': 400, 'oefa': 5, 'campo': 300, 'servicio': 9000}
    label = groups.label(texts, df, 10000, stopwords={'servicio'})
    assert label.split(' · ')[0] == 'tercero'  # la palabra más distintiva del grupo va primero
    assert 'oefa' not in label  # aparece en menos de la mitad del grupo
