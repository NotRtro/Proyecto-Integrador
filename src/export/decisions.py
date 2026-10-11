"""Tabla de decisiones de volumen: qué datos usa cada tarea, por qué y cuánto cuesta.

Motor: local (lee artefactos pequeños). Se arma al final de cada corrida con cifras
reales —tamaños, tiempos y la evidencia de qué se pierde al muestrear— para que la
justificación de docs/DecisionesTecnicas.md no dependa de números escritos a mano.

Artefacto: metadata/data_decisions.json.
"""

import hashlib
import json

import pandas as pd

from src.config import ARTIFACTS, SEED

FRACTIONS = [0.3, 0.1]


def read(name, default=None):
    path = ARTIFACTS / name
    if not path.is_file():
        return default
    if path.suffix == '.json':
        return json.loads(path.read_text(encoding='utf-8'))
    if path.suffix == '.parquet':
        return pd.read_parquet(path)
    return pd.read_csv(path)


def step_seconds(timings, *names):
    """Suma de los tiempos de las subetapas indicadas (lista de {step, seconds})."""
    return round(sum(t['seconds'] for t in timings or [] if t['step'] in names), 1)


def stage_seconds(manifest, name):
    return round(next((s['seconds'] for s in manifest.get('stages', []) if s['name'] == name), 0.0), 1)


def kept(key, fraction):
    """Misma regla para todos: un proceso (o una entidad) entra a la muestra según el hash de su clave."""
    value = int.from_bytes(hashlib.sha256(f'{key}:{SEED}'.encode()).digest()[:4], 'little') % 1000
    return value < fraction * 1000


def q3_evidence(pairs):
    """Pares casi idénticos y entidades que repiten (≤ 30 días) que sobreviven a una muestra.

    Dos diseños: procesos al azar (un par sobrevive si entran sus dos procesos, ≈ p²) y
    entidades completas al azar (un par de la misma entidad sobrevive con probabilidad p;
    uno entre entidades distintas, solo si entran las dos, ≈ p²).
    """
    if pairs is None or pairs.empty:
        return []
    same = pairs.left_buyer_id == pairs.right_buyer_id
    short = pairs.kind == 'misma entidad, <= 30 días'
    buyers = pairs[short].left_buyer_id.nunique()
    designs = {'aleatorio': ('left', 'right'), 'conglomerados': ('left_buyer_id', 'right_buyer_id')}
    rows = []
    for design, (left, right) in designs.items():
        for fraction in FRACTIONS:
            both = pairs[left].map(lambda k: kept(k, fraction)) & pairs[right].map(lambda k: kept(k, fraction))
            rows.append(
                dict(
                    design=design,
                    fraction=fraction,
                    pairs_full=len(pairs),
                    pairs_kept=int(both.sum()),
                    pairs_kept_share=float(both.mean()),
                    expected_share=fraction**2 if design == 'aleatorio' else None,
                    same_buyer_kept_share=float(both[same].mean()),
                    cross_buyer_full=int((~same).sum()),
                    cross_buyer_kept_share=float(both[~same].mean()) if (~same).any() else None,
                    repeating_buyers_full=int(buyers),
                    repeating_buyers_detected=int(pairs[short & both].left_buyer_id.nunique()),
                )
            )
    return rows


def q1_evidence(effect):
    if effect is None or effect.empty:
        return []
    if 'design' not in effect:  # mediciones anteriores: solo diseño aleatorio
        effect = effect.assign(design=effect.fraction.map(lambda f: 'todos' if f >= 1 else 'aleatorio'))
    full = effect[effect.design == 'todos'].set_index('year')
    rows = []
    for _, r in effect[effect.design != 'todos'].iterrows():
        base = full.loc[r.year]
        rows.append(
            dict(
                design=r.design,
                fraction=r.fraction,
                year=int(r.year),
                buyers_full=int(base.buyers_evaluable),
                buyers_sample=int(r.buyers_evaluable),
                buyers_kept_share=r.buyers_evaluable / base.buyers_evaluable,
                median_hhi_full=base.median_hhi,
                median_hhi_sample=r.median_hhi,
                high_share_full=base.high_share,
                high_share_sample=r.high_share,
                same_top_supplier=r.same_top_supplier,
                high_recall=r.get('high_recall'),
                high_precision=r.get('high_precision'),
            )
        )
    return rows


def ann_evidence(scaling, methodology):
    if scaling is None or scaling.empty:
        return {}
    full, reduced = scaling.iloc[-1], scaling[scaling.corpus_size <= 50_000].iloc[-1]
    return dict(
        corpus_reduced=int(reduced.corpus_size),
        corpus_full=int(full.corpus_size),
        hnsw_recall_reduced=reduced.hnsw_recall_at_10,
        hnsw_recall_full=full.hnsw_recall_at_10,
        speedup_reduced=reduced.exact_mean_ms / reduced.hnsw_mean_ms,
        speedup_full=full.exact_mean_ms / full.hnsw_mean_ms,
        evaluated_corpus=(methodology or {}).get('corpus_size'),
        profile=(methodology or {}).get('profile'),
    )


def build():
    manifest = read('metadata/run_manifest.json', {})
    quality = read('metadata/quality_summary.json', {})
    part1 = read('eda/part1_timings.json', [])
    part2 = read('similarity/part2_timings.json', [])
    part3 = read('ann/part3_timings.json', [])
    part5 = read('association/part5_timings.json', [])
    similarity = read('similarity/methodology.json', {})
    ann = read('ann/methodology.json', {})
    experiments = read('metadata/experiments.json', {})
    processes = quality.get('selected_processes')
    everything = f'todos ({processes:,} procesos)'.replace(',', ' ') if processes else 'todos'
    tasks = [
        dict(
            task='Parseo, limpieza y tablas',
            question='todas',
            data=f"todos ({quality.get('raw_records', 0):,} registros)".replace(',', ' '),
            reason='Es la base de todo y la tarea que justifica Spark por volumen (JSON anidado de ~3 GB).',
            seconds=stage_seconds(manifest, 'processing'),
            engine='Spark',
        ),
        dict(
            task='Concentración HHI',
            question='Q1',
            data=everything,
            reason='El HHI por entidad necesita todas sus adjudicaciones: con procesos al azar muchas entidades '
            'no llegan al mínimo y el HHI se infla; con entidades completas el HHI es exacto pero solo se '
            'encuentra una fracción p de las entidades concentradas (ver evidencia).',
            seconds=step_seconds(part1, 'concentración (Q1)'),
            engine='Spark',
        ),
        dict(
            task='Tasa de postor único y EDA',
            question='Q2',
            data=everything,
            reason='Los grupos de interés (modalidad × departamento) son pequeños; una muestra los vacía.',
            seconds=step_seconds(part1, 'EDA y cubos'),
            engine='Spark',
        ),
        dict(
            task='MinHash + LSH sobre el corpus',
            question='Q3',
            data=f"todos ({similarity.get('full_corpus', {}).get('documents', 0):,} con texto)".replace(',', ' '),
            reason='Con procesos al azar sobrevive el p² de los pares; con entidades completas, el p de las '
            'repeticiones de una entidad y el p² de las que cruzan entidades (ver evidencia).',
            seconds=step_seconds(part2, 'LSH sobre el corpus completo', 'requerimientos repetidos (Q3)'),
            engine='Spark',
        ),
        dict(
            task='Jaccard exacto de referencia',
            question='Parte II',
            data=f"muestra por bloques ({similarity.get('sample', {}).get('size', 0):,} procesos)".replace(',', ' '),
            reason='Es cuadrático: con 1 500 procesos son 1,1 millones de pares; con el corpus serían 19 mil millones. '
            'Solo sirve de verdad para medir el error de MinHash y LSH.',
            seconds=step_seconds(part2, 'evaluación en muestra (MinHash y rejilla LSH)'),
            engine='local + Spark',
        ),
        dict(
            task='Evaluación de ANN (HNSW, IVF)',
            question='Parte III',
            data=f"{ann.get('corpus_size', 0):,} procesos (perfil {ann.get('profile', 'completo')})".replace(',', ' '),
            reason='Compara algoritmos, no responde una pregunta sobre el Estado: el recall y la aceleración se '
            'sostienen con 50 mil procesos (ver evidencia del escalamiento).',
            seconds=step_seconds(
                part3,
                'lectura y vectorización TF-IDF + SVD',
                'búsqueda exacta (referencia)',
                'rejilla HNSW',
                'rejilla IVF',
            ),
            engine='local',
        ),
        dict(
            task='Bloom, Count-Min y DGIM',
            question='Q1, Q2',
            data='todos los eventos, en orden',
            reason='Un flujo es su historia completa: “¿ya ganó antes con esta entidad?” no tiene sentido sin ella.',
            seconds=stage_seconds(manifest, 'part4'),
            engine='Spark + local',
        ),
        dict(
            task='FP-Growth',
            question='Q2',
            data=everything,
            reason='Es barato sobre todas las canastas y las reglas dirigidas necesitan grupos pequeños.',
            seconds=step_seconds(part5, 'FP-Growth sobre todas las canastas', 'reglas dirigidas a postor único (Q2)'),
            engine='Spark',
        ),
        dict(
            task='Apriori de referencia',
            question='Parte V',
            data='muestra de 10 000 canastas',
            reason='Recorre todas las canastas en cada nivel; solo valida que FP-Growth encuentra lo mismo.',
            seconds=step_seconds(part5, 'Apriori frente a FP-Growth (muestra)'),
            engine='local',
        ),
    ]
    measurements = [
        dict(name=name, **{k: v for k, v in entry.items() if k != 'artifacts'}) for name, entry in experiments.items()
    ]
    return dict(
        profile=manifest.get('profile'),
        experiments_mode=manifest.get('experiments'),
        total_seconds=round(sum(s['seconds'] for s in manifest.get('stages', [])), 1),
        stages=[dict(name=s['name'], seconds=round(s['seconds'], 1)) for s in manifest.get('stages', [])],
        tasks=tasks,
        experiments=measurements,
        evidence=dict(
            q1_sampling=q1_evidence(read('eda/sampling_effect.csv')),
            q3_sampling=q3_evidence(read('similarity/repeated_requirements.parquet')),
            ann_scaling=ann_evidence(read('ann/scaling.csv'), ann),
        ),
    )
