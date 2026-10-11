"""Señales de riesgo por proceso y su coincidencia (pregunta Q4).

Motor: Spark (joins y agregaciones sobre processes, suppliers y los pares repetidos de la Parte II).

No recalcula nada que ya exista: combina por proceso la competencia (Parte I), la
modalidad, la dependencia de un proveedor y los requerimientos repetidos (Parte II).
Cada señal tiene su propio denominador (procesos evaluables). Una señal marca un
proceso para revisión; no prueba ninguna irregularidad.
"""

import json
from itertools import combinations

from pyspark.sql import functions as F

from src.common import processes, save_csv, save_json
from src.config import ARTIFACTS, PROCESSED
from src.part2.repeated import CATEGORIES, SHORT_WINDOW_DAYS

COMPETITIVE = [
    'Adjudicación Simplificada',
    'Subasta Inversa Electrónica',
    'Comparación de Precios',
    'Licitación Pública',
    'Licitación Pública Abreviada',
    'Concurso Público',
    'Concurso Público Abreviado',
]
DIRECT = 'Contratación Directa'
MIN_BUYER_AWARDS = 20
DOMINANT_SHARE = 0.30
MIN_BUYER_PROCESSES = 50
DIMENSIONS = ['category', 'modality', 'amount_band', 'department', 'buyer_level', 'year']

SIGNALS = {
    'single_bidder': dict(
        label='Un solo participante',
        definition='Procedimiento de modalidad competitiva con un único participante reportado en la fuente.',
        evaluable='Procesos de modalidad competitiva con número de participantes informado.',
        caveat='Mercados locales pequeños, bienes de nicho o plazos cortos explican parte de los casos. '
        'No está confirmado si el campo de la fuente cuenta inscritos u ofertas presentadas.',
    ),
    'direct': dict(
        label='Contratación directa',
        definition='El proceso se tramitó como contratación directa, sin concurso.',
        evaluable='Todos los procesos.',
        caveat='Es una modalidad legal (emergencia, proveedor único, contratación entre entidades). '
        'Aquí se mide cuánto se usa, no si su uso estuvo justificado.',
    ),
    'dominant_supplier': dict(
        label='Proveedor dominante',
        definition=f'Ganó un proveedor que concentra al menos el {DOMINANT_SHARE:.0%} del monto adjudicado '
        'por esa entidad en el año.',
        evaluable=f'Procesos adjudicados de entidades con al menos {MIN_BUYER_AWARDS} procesos adjudicados en el año.',
        caveat='Una sola obra o contrato grande puede explicar la participación. Los consorcios reciben un '
        'identificador por contrato y no se pueden seguir entre procesos.',
    ),
    'repeated_purchase': dict(
        label='Compra repetida',
        definition=f'LSH detectó otro procedimiento de la misma entidad con un requerimiento casi idéntico en '
        f'{SHORT_WINDOW_DAYS} días o menos (sin reconvocatorias ni copias del mismo procedimiento).',
        evaluable='Procesos con descripción utilizable.',
        caveat='Los regímenes que contratan servicios individuales en serie y las compras por ítems separados '
        'generan repeticiones legítimas. Distinguirlas de un fraccionamiento exige revisar el expediente. '
        'La búsqueda con LSH no es exhaustiva: la tasa cuenta solo lo detectado.',
    ),
}


def flags(spark):
    """Una fila por proceso con, para cada señal, si es evaluable y si está marcada."""
    base = processes(spark)
    awards = spark.read.parquet(str(PROCESSED / 'suppliers')).filter(
        F.col('allocated_amount_pen').isNotNull() & F.col('supplier_id').isNotNull() & F.col('buyer_id').isNotNull()
    )
    # Proveedor dominante: participación en el monto que la entidad adjudicó ese año
    keys = ['year', 'buyer_id']
    buyers = awards.groupBy(*keys).agg(
        F.sum('allocated_amount_pen').alias('buyer_amount'), F.countDistinct('ocid').alias('buyer_awards')
    )
    share = (
        awards.groupBy(*keys, 'supplier_id')
        .agg(F.sum('allocated_amount_pen').alias('amount'))
        .join(buyers.filter((F.col('buyer_awards') >= MIN_BUYER_AWARDS) & (F.col('buyer_amount') > 0)), keys)
        .withColumn('share', F.col('amount') / F.col('buyer_amount'))
    )
    dominant = (
        awards.select('ocid', *keys, 'supplier_id')
        .distinct()
        .join(share, [*keys, 'supplier_id'])
        .groupBy('ocid')
        .agg(F.max('share').alias('top_share'))
    )
    # Compra repetida: procesos en algún par de la misma entidad en la ventana corta
    pairs = spark.read.parquet(str(ARTIFACTS / 'similarity/repeated_requirements.parquet'))
    short = pairs.filter((F.col('kind') == CATEGORIES[0]) & ~F.col('reconvocation') & ~F.col('same_procedure'))
    repeated = short.select(F.explode(F.array('left', 'right')).alias('ocid')).distinct().withColumn('rep', F.lit(1))
    competitive = F.col('modality').isin(COMPETITIVE)
    return (
        base.join(dominant, 'ocid', 'left')
        .join(repeated, 'ocid', 'left')
        .select(
            'ocid',
            'buyer_id',
            'buyer_name',
            F.col('year').cast('string').alias('year'),
            *[F.coalesce(F.col(c), F.lit('NO INFORMADO')).alias(c) for c in DIMENSIONS if c != 'year'],
            F.when(competitive & F.col('bidders').isNotNull(), (F.col('bidders') == 1).cast('int')).alias(
                'single_bidder'
            ),
            (F.col('modality') == DIRECT).cast('int').alias('direct'),
            (F.col('top_share') >= DOMINANT_SHARE).cast('int').alias('dominant_supplier'),
            F.when(F.length('normalized_text') > 0, F.coalesce('rep', F.lit(0))).alias('repeated_purchase'),
        )
    )


def rates(df, keys):
    """Evaluables, marcados y tasa de cada señal por grupo, en formato largo."""
    aggregations = [F.count('*').alias('processes')]
    for s in SIGNALS:
        aggregations += [F.count(s).alias(f'{s}_evaluable'), F.sum(s).alias(f'{s}_flagged')]
    rows = []
    for r in df.groupBy(*keys).agg(*aggregations).collect():
        for s, meta in SIGNALS.items():
            evaluable, flagged = r[f'{s}_evaluable'], r[f'{s}_flagged'] or 0
            rows.append(
                dict(
                    **{k: r[k] for k in keys},
                    signal=s,
                    label=meta['label'],
                    processes=r['processes'],
                    evaluable=evaluable,
                    flagged=flagged,
                    rate=flagged / evaluable if evaluable else None,
                    not_evaluable_share=1 - evaluable / r['processes'],
                )
            )
    return rows


def cooccurrence(df):
    """Lift entre pares de señales, sobre los procesos evaluables en ambas."""
    rows = []
    for a, b in combinations(SIGNALS, 2):
        r = (
            df.filter(F.col(a).isNotNull() & F.col(b).isNotNull())
            .agg(
                F.count('*').alias('n'),
                F.sum(a).alias('a'),
                F.sum(b).alias('b'),
                F.sum(F.col(a) * F.col(b)).alias('ab'),
            )
            .first()
        )
        n, na, nb, joint = r['n'], r['a'] or 0, r['b'] or 0, r['ab'] or 0
        # Sin procesos comunes o sin casos de una de las dos, el lift no está definido
        expected = na * nb / n if n and na and nb else None
        rows.append(
            dict(
                signal_a=a,
                signal_b=b,
                label_a=SIGNALS[a]['label'],
                label_b=SIGNALS[b]['label'],
                evaluable_both=n,
                flagged_a=na,
                flagged_b=nb,
                joint=joint if expected else None,
                expected_joint=expected,
                lift=joint / expected if expected else None,
            )
        )
    return rows


def run(spark):
    df = flags(spark).cache()
    total = df.count()
    count = sum(F.coalesce(F.col(s), F.lit(0)) for s in SIGNALS)
    df = df.withColumn('signal_count', count)
    # Procesos con un solo participante que no registran adjudicación
    awarded = processes(spark).select('ocid', 'award_count')
    without_award = df.filter(F.col('single_bidder') == 1).join(awarded, 'ocid').filter('award_count = 0').count()
    lsh = json.loads((ARTIFACTS / 'similarity/methodology.json').read_text(encoding='utf-8'))['full_corpus']
    summary = rates(df.withColumn('all', F.lit('Todos')), ['all']) + rates(df, ['year'])
    save_csv('integrity/summary.csv', [dict(year=r.pop('all', None) or r.pop('year'), **r) for r in summary])
    by_dimension = []
    for dimension in DIMENSIONS:
        for r in rates(df, [dimension]):
            by_dimension.append(dict(dimension=dimension, value=r.pop(dimension), **r))
    save_csv('integrity/by_dimension.csv', by_dimension)
    save_csv('integrity/cooccurrence.csv', cooccurrence(df))
    counts = [r.asDict() for r in df.groupBy('signal_count').count().orderBy('signal_count').collect()]
    save_csv(
        'integrity/count_distribution.csv',
        [dict(signal_count=r['signal_count'], processes=r['count'], share=r['count'] / total) for r in counts],
    )
    # Entidades públicas: sí se nombran; a los proveedores no se les asigna ninguna señal
    buyers = (
        df.groupBy('buyer_id')
        .agg(
            F.first('buyer_name').alias('buyer_name'),
            F.first('department').alias('department'),
            F.first('buyer_level').alias('buyer_level'),
            F.count('*').alias('processes'),
            F.avg((F.col('signal_count') > 0).cast('int')).alias('with_signal_share'),
            *[F.count(s).alias(f'{s}_evaluable') for s in SIGNALS],
            *[F.avg(s).alias(f'{s}_rate') for s in SIGNALS],
        )
        .filter(F.col('processes') >= MIN_BUYER_PROCESSES)
    )
    save_csv('integrity/buyers.csv', buyers.toPandas())
    save_json(
        'integrity/methodology.json',
        dict(
            processes=total,
            signals=SIGNALS,
            competitive_modalities=COMPETITIVE,
            excluded_modality='Adjudicación Selectiva o Abreviada no entra en las modalidades competitivas: agrupa '
            'dos procedimientos de regímenes propios cuyo diseño no se pudo confirmar. Es una decisión operativa.',
            single_bidder_without_award=without_award,
            repeated_detection=dict(
                method='Pares candidatos de LSH sobre todo el corpus, verificados con Jaccard exacto.',
                threshold=lsh['threshold'],
                max_bucket=lsh['max_bucket'],
                oversized_buckets=lsh['oversized_buckets'],
                largest_bucket=lsh['largest_bucket'],
                note='LSH puede omitir pares verdaderos y los buckets mayores que max_bucket no se expanden. '
                'La omisión no se cuantificó: la tasa es de repetición detectada, no de repetición total.',
            ),
            min_buyer_awards=MIN_BUYER_AWARDS,
            dominant_share=DOMINANT_SHARE,
            min_buyer_processes=MIN_BUYER_PROCESSES,
            principle='Una señal marca un proceso que conviene revisar; no prueba ninguna irregularidad.',
            cooccurrence='Lift = coincidencias observadas / esperadas si las dos señales fueran independientes, '
            'calculado solo sobre los procesos evaluables en ambas.',
            privacy='Se nombran entidades públicas. No se asignan señales a proveedores: muchos son personas '
            'naturales.',
        ),
    )
    df.unpersist()
