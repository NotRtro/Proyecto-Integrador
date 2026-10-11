"""Parte V: conjuntos frecuentes y reglas de asociación (Apriori y FP-Growth).

Motor: FP-Growth de Spark MLlib sobre todas las canastas; Apriori propio en el driver
sobre una muestra, para verificar que ambos encuentran exactamente los mismos conjuntos.

Pasos:
1. Apriori frente a FP-Growth sobre la misma muestra (soportes 2 %, 5 % y 10 %): mismos
   conjuntos o la corrida falla; se comparan tiempos.
2. FP-Growth sobre todas las canastas: conjuntos frecuentes y reglas (soporte >= 2 %,
   confianza >= 50 %), cada una con su tipo (informativa, normativa, datos faltantes),
   su improvement frente a sus sub-reglas y sus métricas recalculadas por año. Para
   interpretar se eligen reglas informativas con improvement >= 5 puntos.
3. Reglas dirigidas (Q2): ¿qué combinaciones elevan la probabilidad de tener un solo
   postor (y, como contraste, cuatro o más) donde debería haber competencia? Se minan
   procesos con postores informados, sin las modalidades no competitivas por diseño
   (más de 50 % de postor único) y con soporte bajo (0,2 %): los grupos son pequeños.
4. Calidad: proporción de "sin dato" por atributo y año.

Artefactos (artifacts/association/): algorithm_benchmark.csv, frequent_itemsets_*.csv,
rules.csv, single_bidder_rules.csv, missing_report.csv, selected_rules.json.
"""

import json
import time

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from src.common import StepTimer, sample_processes, save_csv, save_json
from src.common import features as read_features
from src.config import APRIORI_SAMPLE, spark_session
from src.part5.apriori import apriori
from src.part5.rules import automatic_reading, improvement, metrics_by_year, pick, rule_kind, targeted_rules
from src.part5.transactions import ATTRIBUTES, MISSING, transaction

SUPPORTS = [0.02, 0.05, 0.1]
RULE_SUPPORT = 0.02
RULE_CONFIDENCE = 0.5
TARGET_SUPPORT = 0.002
TARGET_MIN_COUNT = 100
TARGET_MIN_LIFT = 1.2
TARGETS = ['POSTORES=uno', 'POSTORES=cuatro o mas']
NON_COMPETITIVE_RATE = 0.5
SELECTED_GENERAL = 5
SELECTED_SINGLE_BIDDER = 5
# Una regla general seleccionada debe superar en 5 puntos de confianza a toda sub-regla.
MIN_IMPROVEMENT = 0.05


def baskets(spark):
    columns = ['ocid', 'year'] + [column for _, column in ATTRIBUTES]
    rows = read_features(spark).select(*columns).rdd.map(lambda r: (r.ocid, int(r.year), transaction(r)))
    return spark.createDataFrame(rows, 'ocid string, year int, items array<string>')


def compare_algorithms(spark):
    """Apriori y FP-Growth sobre exactamente las mismas canastas de la muestra."""
    sample = [transaction(row) for _, row in sample_processes(spark, APRIORI_SAMPLE).iterrows()]
    frame = spark.createDataFrame([(t,) for t in sample], 'items array<string>').cache()
    frame.count()
    rows, itemsets = [], []
    for support in SUPPORTS:
        start = time.perf_counter()
        exact = apriori(sample, support)
        apriori_seconds = time.perf_counter() - start
        start = time.perf_counter()
        model = FPGrowth(itemsCol='items', minSupport=support).fit(frame)
        found = {frozenset(r['items']): r['freq'] for r in model.freqItemsets.collect()}
        fpgrowth_seconds = time.perf_counter() - start
        if found != exact:
            raise AssertionError(f'Apriori y FP-Growth difieren con soporte {support}')
        for algorithm, seconds in [('Apriori', apriori_seconds), ('FP-Growth', fpgrowth_seconds)]:
            rows.append(
                dict(
                    algorithm=algorithm,
                    scope='same_sample',
                    support=support,
                    seconds=seconds,
                    transactions=len(sample),
                    itemsets=len(exact),
                    equivalent=True,
                )
            )
        itemsets.extend(
            dict(
                min_support=support,
                items=json.dumps(sorted(k), ensure_ascii=False),
                frequency=v,
                support=v / len(sample),
            )
            for k, v in exact.items()
        )
    frame.unpersist()
    return rows, itemsets


def full_rules(tx, total):
    """FP-Growth sobre todas las canastas: conjuntos por soporte y reglas al 2 %."""
    benchmark, itemsets, rules = [], [], []
    for support in SUPPORTS:
        start = time.perf_counter()
        model = FPGrowth(itemsCol='items', minSupport=support, minConfidence=RULE_CONFIDENCE).fit(tx)
        found = model.freqItemsets.collect()
        raw_rules = model.associationRules.collect() if support == RULE_SUPPORT else []
        benchmark.append(
            dict(
                algorithm='FP-Growth',
                scope='full',
                support=support,
                seconds=time.perf_counter() - start,
                transactions=total,
                itemsets=len(found),
                equivalent=None,
            )
        )
        itemsets.extend(
            dict(
                min_support=support,
                items=json.dumps(sorted(r['items']), ensure_ascii=False),
                frequency=r['freq'],
                support=r['freq'] / total,
            )
            for r in found
        )
        rules.extend(raw_rules)
    frequencies = {
        frozenset(json.loads(r['items'])): r['frequency'] for r in itemsets if r['min_support'] == RULE_SUPPORT
    }
    return benchmark, itemsets, rules, frequencies


def rules_by_year(raw_rules, tx, frequencies, total):
    """Cada regla con sus métricas globales y por año (recontadas en las canastas del año).

    El improvement se calcula sobre todas las canastas (vacío en las filas por año).
    """
    grouped = {}
    for row in tx.groupBy('year', 'items').count().collect():
        grouped.setdefault(str(row['year']), []).append((set(row['items']), row['count']))
    grouped = {'Todos': sum(grouped.values(), []), **grouped}
    rows = []
    for rule in raw_rules:
        antecedent, consequent = set(rule.antecedent), set(rule.consequent)
        if rule.lift <= 1:
            continue
        target = next(iter(consequent)) if len(consequent) == 1 else None
        kind = rule_kind(antecedent | consequent, target)
        gain = improvement(antecedent, target, frequencies, total) if target else None
        for metrics in metrics_by_year(antecedent, consequent, grouped):
            rows.append(
                dict(
                    antecedent=' ∧ '.join(sorted(antecedent)),
                    consequent=' ∧ '.join(sorted(consequent)),
                    kind=kind,
                    antecedent_size=len(antecedent),
                    size=len(antecedent | consequent),
                    improvement=gain if metrics['year'] == 'Todos' else None,
                    **metrics,
                )
            )
    return rows


def non_competitive_modalities(informed):
    """Modalidades donde más de la mitad de los procesos tiene un solo postor.

    Son no competitivas por diseño (contratación directa, convenios, contratación
    internacional, regímenes especiales): incluirlas en las reglas dirigidas solo
    redescubriría que allí hay un único proveedor. Se calcula de los datos, no se fija a mano.
    """
    modality = F.filter('items', lambda i: i.startswith('MODALIDAD='))[0]
    single = F.array_contains('items', 'POSTORES=uno').cast('int')
    rows = (
        informed.groupBy(modality.alias('modality')).agg(F.avg(single).alias('rate'), F.count('*').alias('n')).collect()
    )
    return sorted(
        (
            dict(modality=r['modality'], single_bidder_rate=r['rate'], processes=r['n'])
            for r in rows
            if r['rate'] > NON_COMPETITIVE_RATE
        ),
        key=lambda e: -e['single_bidder_rate'],
    )


def single_bidder_rules(tx):
    """Reglas dirigidas a POSTORES=uno (y su contraste) donde debería haber competencia.

    Se excluyen los procesos sin postores informados y los de modalidades no competitivas
    por diseño (ver non_competitive_modalities).
    """
    informed = tx.filter(~F.array_contains('items', f'POSTORES={MISSING}')).cache()
    excluded = non_competitive_modalities(informed)
    competitive = informed
    if excluded:
        competitive = informed.filter(~F.arrays_overlap('items', F.array(*[F.lit(e['modality']) for e in excluded])))
    total = competitive.count()
    model = FPGrowth(itemsCol='items', minSupport=TARGET_SUPPORT).fit(competitive)
    itemsets = {frozenset(r['items']): r['freq'] for r in model.freqItemsets.collect()}
    informed.unpersist()
    rows = []
    for target in TARGETS:
        rows.extend(targeted_rules(itemsets, total, target, TARGET_MIN_COUNT, TARGET_MIN_LIFT))
    return rows, total, excluded


def missing_report(tx):
    """Proporción de "sin dato" por atributo y año."""
    exploded = tx.select('year', F.explode('items').alias('item'))
    parts = F.split('item', '=')
    return (
        exploded.withColumn('attribute', parts[0])
        .withColumn('missing', (parts[1] == MISSING).cast('int'))
        .groupBy('attribute', 'year')
        .agg(F.count('*').alias('processes'), F.sum('missing').alias('missing'))
        .withColumn('missing_share', F.col('missing') / F.col('processes'))
        .orderBy('attribute', 'year')
    )


def select_rules(general, targeted):
    """Ocho o más reglas para interpretar: informativas generales y de postor único, sin redundancia."""
    chosen = []
    candidates = sorted(
        (
            r
            for r in general
            if r['year'] == 'Todos'
            and r['kind'] == 'informativa'
            and r['support'] >= RULE_SUPPORT
            and r['confidence'] >= RULE_CONFIDENCE
            and r['improvement'] >= MIN_IMPROVEMENT
            and r['size'] <= 3
        ),
        key=lambda r: (-r['lift'], -r['support']),
    )
    pick(candidates, SELECTED_GENERAL, 'general', chosen)
    single = [
        r for r in targeted if r['consequent'] == 'POSTORES=uno' and r['kind'] == 'informativa' and r['improvement'] > 0
    ]
    pick(single, SELECTED_SINGLE_BIDDER, 'postor único (Q2)', chosen, unique_items=True)
    for rule in chosen:
        rule.pop('_items')
        rule.pop('_antecedent')
        rule['automatic_reading'] = automatic_reading(rule)
        rule['rule_id'] = f"{rule['antecedent']} → {rule['consequent']}"
    return chosen


def run(spark):
    timer = StepTimer()
    tx = baskets(spark).cache()
    total = tx.count()
    with timer.step('Apriori frente a FP-Growth (muestra)'):
        benchmark, sample_itemsets = compare_algorithms(spark)
    with timer.step('FP-Growth sobre todas las canastas'):
        full_benchmark, full_itemsets, raw_rules, frequencies = full_rules(tx, total)
    with timer.step('reglas por año'):
        general = rules_by_year(raw_rules, tx, frequencies, total)
    with timer.step('reglas dirigidas a postor único (Q2)'):
        targeted, informed, non_competitive = single_bidder_rules(tx)
    with timer.step('calidad de las canastas'):
        missing = missing_report(tx).toPandas()

    save_csv('association/algorithm_benchmark.csv', benchmark + full_benchmark)
    save_csv('association/frequent_itemsets_apriori.csv', sample_itemsets)
    save_csv('association/frequent_itemsets_fpgrowth.csv', full_itemsets)
    save_csv(
        'association/rules.csv',
        general,
        [
            'year',
            'antecedent',
            'consequent',
            'kind',
            'support',
            'confidence',
            'lift',
            'improvement',
            'antecedent_size',
            'size',
            'count',
            'transactions',
        ],
    )
    save_csv('association/single_bidder_rules.csv', targeted)
    save_csv('association/missing_report.csv', missing)
    selected = select_rules(general, targeted)
    save_json(
        'association/selected_rules.json',
        dict(
            rules=selected,
            required=8,
            found=len(selected),
            status='complete' if len(selected) >= 8 else 'insufficient_evidence',
            note='automatic_reading es una lectura literal de las métricas. La interpretación en contexto la escribe '
            'el '
            'equipo en frontend/src/content/rules.json, usando rule_id como clave.',
            transactions=total,
            competitive_transactions=informed,
            non_competitive_modalities=non_competitive,
        ),
    )
    save_json('association/part5_timings.json', timer.steps)
    tx.unpersist()


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
