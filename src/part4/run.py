"""Parte IV: minería de flujos con Bloom Filter, Count-Min Sketch y DGIM.

Motor: Spark arma y ordena los flujos; las estructuras procesan los eventos uno a uno en
el driver (`toLocalIterator`), como lo haría un sistema que los recibe en tiempo real.
Un flujo es secuencial por definición: aquí lo distribuido es la preparación, no el
resumen.

Flujos (ordenados por fecha):
- Adjudicaciones: un evento por proveedor adjudicado (fecha de la adjudicación o, si
  falta, de la convocatoria), con la entidad, el proveedor y si el proceso tuvo un solo
  postor o un monto convocado >= S/ 1 millón.
- Ítems: un evento por ítem con su código UNSPSC.

Preguntas:
- Bloom (Q1): ¿este proveedor ya había ganado antes con esta entidad? Clave = par
  (entidad, proveedor). Se consulta antes de insertar; un "sí" para un par nuevo es un
  falso positivo.
- Count-Min Sketch: ¿cuántas adjudicaciones lleva cada proveedor y cuántas veces se
  pidió cada código UNSPSC? Decenas de miles de claves distintas.
- DGIM (Q2): ¿cuántos procesos con un solo postor (o de alto monto) hubo en la ventana
  reciente? Ventanas de N eventos y de N días.
"""
import sys
import time
from collections import Counter, deque
from datetime import date

import numpy as np
from pyspark.sql import functions as F

from src.common import StepTimer, processes as read_processes, save_csv, save_json
from src.config import PROCESSED, spark_session
from src.part4.bloom_filter import BloomFilter, optimal_hashes, theoretical_fpr
from src.part4.count_min_sketch import CountMinSketch, delta, epsilon
from src.part4.dgim import DGIM

BLOOM_BITS = [2 ** 17, 2 ** 19, 2 ** 21]
BLOOM_HASHES = [3, 5, 7]
CMS_WIDTHS = [512, 2048, 8192]
CMS_DEPTHS = [3, 5]
DGIM_CONFIGS = [('eventos', 1000), ('eventos', 10000), ('días', 7), ('días', 30)]
DGIM_SIGNALS = {'postor_unico': 'single_bidder', 'alto_monto': 'high_amount'}
HIGH_AMOUNT = 1_000_000
SERIES_EVERY = 2000
HEAVY_HITTERS = 100
CHART_KEYS = 30


def award_events(spark):
    suppliers = spark.read.parquet(str(PROCESSED / 'suppliers')).select(
        'ocid', 'award_id', 'supplier_id', 'buyer_id', 'conversion_reference_date')
    context = read_processes(spark).select('ocid', 'effective_date', 'competition', 'tender_amount_pen')
    return (suppliers.join(context, 'ocid')
            .withColumn('event_date', F.coalesce('conversion_reference_date', 'effective_date'))
            .filter(F.col('event_date').isNotNull() & F.col('supplier_id').isNotNull())
            .select('event_date', 'ocid', 'award_id', 'buyer_id', 'supplier_id',
                    (F.col('competition') == 'uno').alias('single_bidder'),
                    (F.coalesce('tender_amount_pen', F.lit(0.0)) >= HIGH_AMOUNT).alias('high_amount'))
            .orderBy('event_date', 'ocid', 'award_id', 'supplier_id'))


def item_events(spark):
    items = spark.read.parquet(str(PROCESSED / 'items')).select('conversion_reference_date', 'ocid', 'item_id',
                                                                'classification')
    return (items.filter(F.col('classification').isNotNull() & F.col('conversion_reference_date').isNotNull())
            .orderBy('conversion_reference_date', 'ocid', 'item_id'))


def approximate_set_bytes(values):
    return sys.getsizeof(values) + sum(sys.getsizeof(v) for v in values)


def approximate_counter_bytes(counter):
    return sys.getsizeof(counter) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in counter.items())


def day_number(value):
    return date.fromisoformat(str(value)[:10]).toordinal()


def process_awards(events):
    """Recorre el flujo de adjudicaciones una vez, actualizando todas las estructuras."""
    blooms = {(m, k): BloomFilter(m, k) for m in BLOOM_BITS for k in BLOOM_HASHES}
    bloom_stats = {key: Counter() for key in blooms}
    bloom_time = Counter()
    seen = set()
    sketches = {(w, d): CountMinSketch(w, d) for w in CMS_WIDTHS for d in CMS_DEPTHS}
    cms_time = Counter()
    supplier_counts = Counter()
    dgims = {(signal, unit, size): DGIM(size) for signal in DGIM_SIGNALS for unit, size in DGIM_CONFIGS}
    windows = {key: deque() for key in dgims}
    window_sums = Counter()
    dgim_time = Counter()
    repeat_by_year = Counter()
    bloom_series, dgim_series = [], []
    n = 0
    for n, event in enumerate(events, 1):
        pair = f'{event.buyer_id}|{event.supplier_id}'
        known = pair in seen
        year = str(event.event_date)[:4]
        repeat_by_year[(year, 'events')] += 1
        repeat_by_year[(year, 'repeats')] += known
        for key, bloom in blooms.items():
            start = time.perf_counter()
            predicted = pair in bloom
            bloom.add(pair)
            bloom_time[key] += time.perf_counter() - start
            stats = bloom_stats[key]
            stats['queries'] += 1
            stats['negative_queries'] += not known
            stats['fp'] += predicted and not known
            stats['fn'] += known and not predicted
        seen.add(pair)

        supplier_counts[event.supplier_id] += 1
        for key, sketch in sketches.items():
            start = time.perf_counter()
            sketch.add(event.supplier_id)
            cms_time[key] += time.perf_counter() - start

        today = day_number(event.event_date)
        for key, dgim in dgims.items():
            signal, unit, size = key
            bit = bool(getattr(event, DGIM_SIGNALS[signal]))
            stamp = today if unit == 'días' else n
            start = time.perf_counter()
            dgim.add(bit, stamp)
            dgim_time[key] += time.perf_counter() - start
            window = windows[key]
            window.append((stamp, bit))
            window_sums[key] += bit
            while window and window[0][0] <= stamp - size:
                window_sums[key] -= window.popleft()[1]

        if n % SERIES_EVERY == 0:
            for (m, k), bloom in blooms.items():
                stats = bloom_stats[(m, k)]
                bloom_series.append(dict(event=n, bits=m, hashes=k, inserted=len(seen),
                                         observed_fpr=stats['fp'] / max(1, stats['negative_queries']),
                                         theoretical_fpr=theoretical_fpr(m, k, len(seen))))
            for key, dgim in dgims.items():
                exact, estimate = window_sums[key], dgim.query()
                dgim_series.append(dict(event=n, date=str(event.event_date), signal=key[0], unit=key[1], window=key[2],
                                        exact=exact, estimate=estimate, buckets=len(dgim.buckets),
                                        relative_error=abs(estimate - exact) / exact if exact else None))
        if n % 25000 == 0:
            print(f'Flujos: {n:,} adjudicaciones', flush=True)

    exact_bytes = approximate_set_bytes(seen)
    bloom_rows = []
    for (m, k), bloom in blooms.items():
        stats = bloom_stats[(m, k)]
        bloom_rows.append(dict(bits=m, hashes=k, optimal_hashes=optimal_hashes(m, len(seen)), inserted=len(seen),
                               events=n, **stats, observed_fpr=stats['fp'] / max(1, stats['negative_queries']),
                               theoretical_fpr=bloom.theoretical_fpr(len(seen)), bytes=bloom.bytes,
                               exact_bytes=exact_bytes, operation_us=1e6 * bloom_time[(m, k)] / max(1, n)))
    dgim_rows = []
    for key, dgim in dgims.items():
        exact, estimate = window_sums[key], dgim.query()
        dgim_rows.append(dict(signal=key[0], unit=key[1], window=key[2], exact=exact, estimate=estimate,
                              absolute_error=abs(estimate - exact), relative_error=abs(estimate - exact) / exact if exact else None,
                              buckets=len(dgim.buckets), bytes=dgim.bytes, exact_bytes=16 * len(windows[key]),
                              operation_us=1e6 * dgim_time[key] / max(1, n)))
    repeats = [dict(year=y, events=repeat_by_year[(y, 'events')], repeats=repeat_by_year[(y, 'repeats')],
                    repeat_rate=repeat_by_year[(y, 'repeats')] / repeat_by_year[(y, 'events')])
               for y in sorted({y for y, _ in repeat_by_year})]
    return dict(events=n, bloom=bloom_rows, bloom_series=bloom_series, dgim=dgim_rows, dgim_series=dgim_series,
                repeats=repeats, supplier_sketches=sketches, supplier_counts=supplier_counts, cms_time=cms_time)


def cms_items(events):
    sketches = {(w, d): CountMinSketch(w, d) for w in CMS_WIDTHS for d in CMS_DEPTHS}
    counts = Counter()
    timings = Counter()
    for event in events:
        counts[event.classification] += 1
        for key, sketch in sketches.items():
            start = time.perf_counter()
            sketch.add(event.classification)
            timings[key] += time.perf_counter() - start
    return sketches, counts, timings


def evaluate_cms(domain, sketches, counts, timings):
    """Error por configuración: heavy hitters frente a cola y fracción dentro de la cota ε·N."""
    keys = list(counts)
    exact = np.array([counts[k] for k in keys])
    order = np.argsort(-exact)
    heavy = order[:HEAVY_HITTERS]
    tail = exact <= np.percentile(exact, 50)
    total = int(exact.sum())
    rows, points = [], []
    exact_bytes = approximate_counter_bytes(counts)
    for (w, d), sketch in sketches.items():
        start = time.perf_counter()
        estimate = np.array([sketch.query(k) for k in keys])
        query_us = 1e6 * (time.perf_counter() - start) / len(keys)
        error = estimate - exact
        bound = epsilon(w) * total
        rows.append(dict(domain=domain, width=w, depth=d, events=total, distinct=len(keys), bytes=sketch.bytes,
                         exact_bytes=exact_bytes, epsilon=epsilon(w), delta=delta(d), error_bound=bound,
                         within_bound=float((error <= bound).mean()), guaranteed=1 - delta(d),
                         mean_abs_error=float(error.mean()), max_abs_error=int(error.max()),
                         heavy_mean_relative_error=float((error[heavy] / exact[heavy]).mean()),
                         tail_mean_relative_error=float((error[tail] / exact[tail]).mean()),
                         exact_share=float((error == 0).mean()), underestimates=int((error < 0).sum()),
                         update_us=1e6 * timings[(w, d)] / max(1, total), query_us=query_us))
        for rank, i in enumerate(order[:CHART_KEYS], 1):
            points.append(dict(domain=domain, width=w, depth=d, rank=rank, key=keys[i], exact=int(exact[i]),
                               estimate=int(estimate[i])))
    return rows, points


def run(spark):
    timer = StepTimer()
    with timer.step('flujo de adjudicaciones (Bloom, CMS proveedores, DGIM)'):
        result = process_awards(award_events(spark).toLocalIterator())
    with timer.step('flujo de ítems (CMS UNSPSC)'):
        item_sketches, item_counts, item_times = cms_items(item_events(spark).toLocalIterator())

    cms_rows, cms_points = evaluate_cms('proveedor', result['supplier_sketches'], result['supplier_counts'],
                                        result['cms_time'])
    unspsc_rows, unspsc_points = evaluate_cms('unspsc', item_sketches, item_counts, item_times)
    save_csv('streaming/bloom_grid.csv', result['bloom'])
    save_csv('streaming/bloom_over_time.csv', result['bloom_series'])
    save_csv('streaming/cms_grid.csv', cms_rows + unspsc_rows)
    save_csv('streaming/cms_top_keys.csv', cms_points + unspsc_points)
    save_csv('streaming/dgim_grid.csv', result['dgim'])
    save_csv('streaming/error_over_time.csv', result['dgim_series'])
    save_csv('streaming/repeat_winners.csv', result['repeats'])
    memory = ([dict(algorithm='Bloom', config=f"{r['bits']} bits / {r['hashes']} hashes", approx_bytes=r['bytes'],
                    exact_bytes=r['exact_bytes']) for r in result['bloom']]
              + [dict(algorithm=f"CMS {r['domain']}", config=f"{r['width']} x {r['depth']}", approx_bytes=r['bytes'],
                      exact_bytes=r['exact_bytes']) for r in cms_rows + unspsc_rows]
              + [dict(algorithm='DGIM', config=f"{r['signal']} · {r['window']} {r['unit']}", approx_bytes=r['bytes'],
                      exact_bytes=r['exact_bytes']) for r in result['dgim']])
    save_csv('streaming/memory_comparison.csv', memory)
    # Datos para recalcular en vivo las curvas teóricas en la web (IV-03).
    save_json('streaming/live_params.json', dict(
        bloom=dict(inserted=result['bloom'][0]['inserted'], events=result['events'],
                   observed=[{k: r[k] for k in ('bits', 'hashes', 'observed_fpr')} for r in result['bloom']]),
        cms={domain: dict(events=sum(counts.values()), distinct=len(counts),
                          top=[c for _, c in counts.most_common(50)])
             for domain, counts in [('proveedor', result['supplier_counts']), ('unspsc', item_counts)]},
        dgim=[dict(signal=s, unit=u, window=w) for s in DGIM_SIGNALS for u, w in DGIM_CONFIGS]))
    save_json('streaming/methodology.json', dict(
        events=result['events'], items=sum(item_counts.values()),
        ordering='Fecha de adjudicación (o de convocatoria si falta), ocid, adjudicación, proveedor.',
        bloom='Clave = (entidad, proveedor). Se consulta antes de insertar: un positivo para un par nunca visto es '
              'falso positivo. FPR observado acumulado sobre primeras apariciones; el teórico usa la ocupación final.',
        cms='Frecuencia de proveedores (adjudicaciones) y de códigos UNSPSC (ítems). Error relativo de los 100 más '
            f'frecuentes frente a la mitad menos frecuente; cota ε·N con ε = e/w válida con probabilidad 1 - e^-d.',
        dgim=f'Bits: proceso con un solo postor; monto convocado >= S/ {HIGH_AMOUNT:,}. Ventanas de eventos y de días.',
        memory='Bloom y CMS: tamaño de sus arreglos. Conjuntos y diccionarios exactos: sys.getsizeof (aproximado, no RSS).',
        timings=timer.steps))


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
