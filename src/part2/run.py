"""Parte II: similitud de requerimientos con shingles, Jaccard, MinHash y LSH.

Motor: Spark para el corpus completo (firmas, buckets, candidatos, verificación y Q3);
el driver solo calcula la verdad exacta de la muestra de evaluación (ver evaluation.py).

Representación (justificación en methodology.json): cada proceso es el conjunto de
shingles de 2 palabras de la descripción del requerimiento y sus ítems, sin la
nomenclatura del proceso y sin stopwords de dominio (src/processing/text_features.py).
Se usa MinHash y no SimHash porque la pregunta es de solapamiento de conjuntos
(Jaccard): SimHash aproxima la similitud coseno de vectores ponderados.

Artefactos (artifacts/similarity/):
    minhash_error.csv, minhash_scatter.csv   Jaccard estimado frente a exacto según k (II-01)
    lsh_parameter_grid.csv, s_curve_observed.csv, similar_pairs.csv, documents.csv
    full_corpus_summary.json, template_groups.csv        LSH sobre todo el corpus (II-02)
    repeated_requirements.parquet, repeated_summary.csv, repeated_top_buyers.csv (Q3)
    lsh_groups.csv, lsh_groups_summary.json  grupos de casi idénticos con etiqueta (Q3)
    case_candidates.json, methodology.json
"""

import json

import pandas as pd

from src.common import StepTimer, save_csv, save_json
from src.common import features as read_features
from src.common import processes as read_processes
from src.config import ARTIFACTS, SAMPLE, SEED, spark_session
from src.part2 import full_corpus, groups, repeated
from src.part2.evaluation import ERROR_HASHES, MAX_HASHES, THRESHOLDS, evaluate
from src.processing.text_features import DOMAIN_STOPWORD_DF, SHINGLE_WIDTH

PAIR_COLUMNS = [
    'config',
    'left_ocid',
    'right_ocid',
    'jaccard',
    'status',
    'left_year',
    'right_year',
    'left_category',
    'right_category',
    'same_buyer',
]


def save_evaluation(result):
    save_csv('similarity/minhash_error.csv', result['error'])
    save_csv('similarity/minhash_scatter.csv', result['scatter'])
    save_csv('similarity/lsh_parameter_grid.csv', result['grid'])
    save_csv('similarity/precision_recall_curve.csv', result['grid'])
    save_csv('similarity/s_curve_observed.csv', result['curve'])
    pairs = pd.DataFrame(result['pairs'], columns=PAIR_COLUMNS)
    save_csv('similarity/similar_pairs.csv', pairs)
    save_csv('similarity/false_positives.csv', pairs[pairs.status == 'FP'])
    save_csv('similarity/false_negatives.csv', pairs[pairs.status == 'FN'])
    save_csv('similarity/documents.csv', result['data'][['ocid', 'year', 'category', 'buyer_id', 'text']])


def methodology(result, summary):
    truth = {
        row['threshold']: row['truth_pairs'] for row in result['grid'] if row['hashes'] == 128 and row['rows'] == 4
    }
    return dict(
        representation=f'Shingles de {SHINGLE_WIDTH} palabras de la descripción del requerimiento y sus ítems; '
        'sin la nomenclatura del proceso (tender_code) ni stopwords de dominio '
        f'(frecuencia documental > {DOMAIN_STOPWORD_DF:.0%}).',
        why_minhash='La pregunta es de solapamiento de conjuntos de palabras (Jaccard). SimHash aproxima la similitud '
        'coseno de vectores ponderados; para eso está la Parte III (TF-IDF).',
        sample=dict(
            method='Entidades completas elegidas por SHA-256(buyer_id + semilla); ninguna aporta más del 20 %.',
            size=len(result['data']),
            buyers=result['buyers'],
            seed=SEED,
            truth_pairs_by_threshold=truth,
        ),
        minhash=dict(
            hashes=ERROR_HASHES,
            max_hashes=MAX_HASHES,
            hash_family='(a·x + b) mod (2^31 - 1)',
            error='Ĵ = proporción de componentes iguales; error estándar teórico sqrt(J(1-J)/k).',
        ),
        lsh=dict(
            metric='Precisión y recall de los candidatos LSH antes de verificar con Jaccard exacto.',
            thresholds=THRESHOLDS,
        ),
        full_corpus=summary,
        timings=dict(
            result['timings'],
            note='La verdad exacta (X·Xᵀ disperso) y las firmas de la muestra se calculan '
            'en el driver; los candidatos LSH de la rejilla, en Spark.',
        ),
    )


def run(spark):
    timer = StepTimer()
    features = read_features(spark).cache()
    processes = read_processes(spark)

    with timer.step('evaluación en muestra (MinHash y rejilla LSH)'):
        result = evaluate(spark, features, SAMPLE)
        save_evaluation(result)

    with timer.step('LSH sobre el corpus completo'):
        summary, similar, template_groups, cached = full_corpus.run_full_corpus(spark, features)
        save_csv('similarity/template_groups.csv', template_groups.assign(years=template_groups.years.map(sorted)))

    with timer.step('requerimientos repetidos (Q3)'):
        pairs = repeated.classify(similar, processes).cache()
        (ARTIFACTS / 'similarity').mkdir(parents=True, exist_ok=True)
        pairs.toPandas().to_parquet(ARTIFACTS / 'similarity' / 'repeated_requirements.parquet', index=False)
        save_csv('similarity/repeated_summary.csv', repeated.summary(pairs).toPandas())
        save_csv('similarity/repeated_top_buyers.csv', repeated.top_buyers(pairs).toPandas())
        save_json(
            'similarity/case_candidates.json',
            dict(
                note='Pares propuestos para revisión en SEACE (buscar por la nomenclatura tender_code). '
                'El equipo copia los revisados a frontend/src/content/similarity_cases.json con su interpretación.',
                cases=repeated.case_candidates(pairs, features),
            ),
        )
    with timer.step('grupos y etiquetas de LSH (Q3)'):
        stopwords = json.loads((ARTIFACTS / 'metadata/domain_stopwords.json').read_text(encoding='utf-8'))
        lsh_groups, groups_summary = groups.build(pairs, features, {t['term'] for t in stopwords['terms']})
        save_csv('similarity/lsh_groups.csv', lsh_groups)
        save_json('similarity/lsh_groups_summary.json', groups_summary)
        pairs.unpersist()

    # Compatibilidad con la API actual: los casos interpretados los escribe el equipo en
    # frontend/src/content/similarity_cases.json (paso 8.7); aquí no se inventan.
    save_json('similarity/reviewed_cases.json', dict(cases=[], review_type='Pendiente de revisión humana'))
    save_json('similarity/methodology.json', methodology(result, summary))
    save_json('similarity/part2_timings.json', timer.steps)
    for df in cached + [features]:
        df.unpersist()
    print(
        json.dumps({k: summary[k] for k in ['documents', 'possible_pairs', 'candidate_pairs', 'similar_pairs']}),
        flush=True,
    )


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
