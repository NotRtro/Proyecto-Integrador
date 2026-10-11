"""Simulación del flujo con Spark Structured Streaming (Parte IV).

Motor: Spark Structured Streaming (fuente de archivos + foreachBatch); las estructuras de
resumen se actualizan en el driver con cada micro-lote.

El recorrido secuencial de run.py mide la precisión de cada estructura. Aquí se simula
cómo llegarían los datos de verdad: el OECE publica adjudicaciones día a día y un
sistema de vigilancia las recibe por lotes.

1. Las adjudicaciones se escriben como un archivo Parquet por semana, con fechas de
   modificación crecientes (la fuente de archivos de Spark los entrega en ese orden).
2. `readStream` lee la carpeta con `maxFilesPerTrigger=1`: cada micro-lote es una semana
   nueva de adjudicaciones. `Trigger.AvailableNow` procesa todo lo publicado y se detiene.
3. `foreachBatch` actualiza un Bloom Filter (¿el proveedor ya ganó con esta entidad?),
   un Count-Min Sketch (adjudicaciones por proveedor) y un DGIM (procesos con un solo
   postor en los últimos 30 días), junto a sus versiones exactas para medir el error
   en cada momento del flujo. El checkpoint permite reanudar sin reprocesar semanas.

Artefactos: streaming/replay_batches.csv (una fila por micro-lote) y
streaming/replay_summary.json (configuración, latencias y totales).
"""

import os
import shutil
import time
from collections import Counter, deque
from datetime import date

import pandas as pd
from pyspark.sql import functions as F

from src.config import PROCESSED
from src.part4.bloom_filter import BloomFilter, theoretical_fpr
from src.part4.count_min_sketch import CountMinSketch
from src.part4.dgim import DGIM

BLOOM_BITS, BLOOM_HASHES = 2**19, 3
CMS_WIDTH, CMS_DEPTH = 2048, 5
DGIM_DAYS = 30
TOP_SUPPLIERS = 10
COLUMNS = ['event_date', 'buyer_id', 'supplier_id', 'single_bidder']


def write_weekly_files(events, directory):
    """Un Parquet por semana (lunes a domingo), en orden y con mtime creciente."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    events = events.assign(week=pd.to_datetime(events.event_date).dt.to_period('W-SUN').dt.start_time.dt.date)
    base = time.time() - 10 * len(events.week.unique())
    weeks = sorted(events.week.unique())
    for i, (week, chunk) in enumerate(events.groupby('week', sort=True)):
        path = directory / f'week={week.isoformat()}.parquet'
        chunk[COLUMNS].to_parquet(path, index=False)
        os.utime(path, (base + 10 * i, base + 10 * i))
    return len(weeks)


class ReplayState:
    """Estructuras de resumen y sus versiones exactas, actualizadas micro-lote a micro-lote."""

    def __init__(self):
        self.bloom = BloomFilter(BLOOM_BITS, BLOOM_HASHES)
        self.sketch = CountMinSketch(CMS_WIDTH, CMS_DEPTH)
        self.dgim = DGIM(DGIM_DAYS)
        self.seen = set()
        self.counts = Counter()
        self.window = deque()
        self.window_sum = 0
        self.events = 0
        self.fp = 0
        self.negatives = 0
        self.rows = []

    def process(self, batch, batch_id):
        start = time.perf_counter()
        # La fuente garantiza el orden entre archivos; dentro de la semana se ordena por fecha.
        events = batch.toPandas().sort_values(['event_date', 'buyer_id', 'supplier_id'])
        if events.empty:
            return
        batch_fp = batch_new = 0
        for e in events.itertuples(index=False):
            pair = f'{e.buyer_id}|{e.supplier_id}'
            known = pair in self.seen
            if not known:
                batch_new += 1
                batch_fp += pair in self.bloom
            self.bloom.add(pair)
            self.seen.add(pair)
            self.sketch.add(e.supplier_id)
            self.counts[e.supplier_id] += 1
            day = date.fromisoformat(str(e.event_date)[:10]).toordinal()
            bit = bool(e.single_bidder)
            self.dgim.add(bit, day)
            self.window.append((day, bit))
            self.window_sum += bit
            while self.window and self.window[0][0] <= day - DGIM_DAYS:
                self.window_sum -= self.window.popleft()[1]
        self.events += len(events)
        self.fp += batch_fp
        self.negatives += batch_new
        top = self.counts.most_common(TOP_SUPPLIERS)
        top_error = sum((self.sketch.query(k) - c) / c for k, c in top) / len(top)
        estimate = self.dgim.query()
        self.rows.append(
            dict(
                batch=batch_id,
                week_start=str(events.event_date.min())[:10],
                week_end=str(events.event_date.max())[:10],
                events=len(events),
                cumulative_events=self.events,
                distinct_pairs=len(self.seen),
                new_pairs=batch_new,
                bloom_fp=batch_fp,
                bloom_fpr_batch=batch_fp / batch_new if batch_new else None,
                bloom_fpr_cumulative=self.fp / self.negatives if self.negatives else None,
                bloom_fpr_theoretical=theoretical_fpr(BLOOM_BITS, BLOOM_HASHES, len(self.seen)),
                cms_top_supplier=top[0][0],
                cms_top_exact=top[0][1],
                cms_top_estimate=self.sketch.query(top[0][0]),
                cms_top10_relative_error=top_error,
                dgim_exact=self.window_sum,
                dgim_estimate=estimate,
                dgim_relative_error=abs(estimate - self.window_sum) / self.window_sum if self.window_sum else None,
                dgim_buckets=len(self.dgim.buckets),
                state_bytes=self.bloom.bytes + self.sketch.bytes + self.dgim.bytes,
                exact_state_items=len(self.seen) + len(self.counts) + len(self.window),
                process_seconds=time.perf_counter() - start,
            )
        )


def replay(spark, events):
    """Reproduce el flujo de adjudicaciones semana a semana con Structured Streaming."""
    root = PROCESSED / 'stream_replay'
    source, checkpoint = root / 'source', root / 'checkpoint'
    shutil.rmtree(checkpoint, ignore_errors=True)
    data = events.select(*COLUMNS).withColumn('event_date', F.col('event_date').cast('string')).toPandas()
    weeks = write_weekly_files(data, source)
    schema = spark.read.parquet(str(source)).schema
    state = ReplayState()
    started = time.perf_counter()
    query = (
        spark.readStream.schema(schema)
        .option('maxFilesPerTrigger', 1)
        .parquet(str(source))
        .writeStream.foreachBatch(state.process)
        .option('checkpointLocation', str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    wall = time.perf_counter() - started
    progress = [p.get('durationMs', {}).get('triggerExecution') for p in query.recentProgress]
    batches = pd.DataFrame(state.rows)
    last = batches.iloc[-1]
    summary = dict(
        engine='Spark Structured Streaming (fuente de archivos Parquet, maxFilesPerTrigger=1, Trigger.AvailableNow, '
        'foreachBatch)',
        batch_unit='una semana de adjudicaciones por micro-lote',
        files=weeks,
        batches=len(batches),
        events=int(batches.events.sum()),
        wall_seconds=wall,
        mean_batch_seconds=float(batches.process_seconds.mean()),
        p95_batch_seconds=float(batches.process_seconds.quantile(0.95)),
        max_events_per_batch=int(batches.events.max()),
        events_per_second=float(batches.events.sum() / wall),
        trigger_ms_recent=[x for x in progress if x is not None][-20:],
        bloom=dict(bits=BLOOM_BITS, hashes=BLOOM_HASHES, final_fpr=last.bloom_fpr_cumulative),
        cms=dict(width=CMS_WIDTH, depth=CMS_DEPTH, final_top10_relative_error=last.cms_top10_relative_error),
        dgim=dict(window_days=DGIM_DAYS, max_relative_error=float(batches.dgim_relative_error.max())),
        state_bytes=int(last.state_bytes),
    )
    return batches, summary
