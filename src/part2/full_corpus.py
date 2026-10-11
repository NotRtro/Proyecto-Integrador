"""MinHash + LSH sobre todo el corpus, sin comparar todos los pares (Parte II, II-02).

Motor: Spark. Es la tarea para la que se justifica Spark en esta parte:
- firmas MinHash en paralelo (`mapPartitions` + numpy en cada ejecutor);
- bandas y buckets con Spark SQL (`transform`, `slice`, `xxhash64`, `groupBy`);
- pares candidatos solo dentro de cada bucket, y verificación con Jaccard exacto
  (`array_intersect` / `array_union`) solo para esos candidatos.

Buckets enormes: un texto-plantilla idéntico en miles de procesos (por ejemplo el mismo
servicio contratado por cientos de municipalidades) cae en el mismo bucket en todas las
bandas. Expandirlo en pares sería cuadrático y no aporta: se reporta aparte como grupo
"texto común a muchos procesos", con cuántas entidades lo comparten.
"""

import time

from pyspark.sql import functions as F

from src.config import SEED
from src.part2.minhash_lsh import coefficients, effective_threshold, signature

HASHES = 128
BANDS = 16
ROWS = 8  # umbral efectivo (1/16)^(1/8) ≈ 0.71: casi duplicados
THRESHOLD = 0.7
MAX_BUCKET = 200  # buckets más grandes no se expanden en pares (ver docstring)
TOP_TEMPLATES = 50


def signatures(spark, features):
    """DataFrame (ocid, sig) con la firma MinHash de cada proceso con texto."""
    coeffs = coefficients(HASHES, SEED)

    def sign(rows):
        for row in rows:
            yield row.ocid, [int(v) for v in signature(row.shingles, coeffs=coeffs)]

    docs = features.filter(F.size('shingles') > 0).select('ocid', 'shingles')
    return spark.createDataFrame(docs.rdd.mapPartitions(sign), 'ocid string, sig array<long>')


def band_buckets(sigs):
    """(banda, clave, documentos, tamaño) para cada bucket LSH."""
    keys = F.transform(
        F.sequence(F.lit(0), F.lit(BANDS - 1)), lambda band: F.xxhash64(F.slice('sig', band * ROWS + 1, ROWS))
    )
    exploded = sigs.select('ocid', F.posexplode(keys).alias('band', 'key'))
    return exploded.groupBy('band', 'key').agg(F.collect_list('ocid').alias('docs'), F.count('*').alias('size'))


def candidate_pairs(buckets):
    """Pares (left < right) de los buckets con 2..MAX_BUCKET documentos, sin repetir."""
    usable = buckets.filter(F.col('size').between(2, MAX_BUCKET))
    pairs = usable.select(
        F.explode(
            F.expr(
                'flatten(transform(sequence(0, size(docs) - 2), '
                'i -> transform(sequence(i + 1, size(docs) - 1), j -> named_struct("a", docs[i], "b", docs[j]))))'
            )
        ).alias('p')
    )
    return pairs.select(F.least('p.a', 'p.b').alias('left'), F.greatest('p.a', 'p.b').alias('right')).distinct()


def verify(pairs, features):
    """Jaccard exacto de cada candidato, calculado en Spark."""
    shingles = features.select('ocid', 'shingles')
    joined = pairs.join(shingles.withColumnRenamed('ocid', 'left').withColumnRenamed('shingles', 'a'), 'left').join(
        shingles.withColumnRenamed('ocid', 'right').withColumnRenamed('shingles', 'b'), 'right'
    )
    return joined.select(
        'left', 'right', (F.size(F.array_intersect('a', 'b')) / F.size(F.array_union('a', 'b'))).alias('jaccard')
    )


def templates(buckets, features):
    """Buckets que superan MAX_BUCKET: textos casi idénticos repetidos en muchos procesos."""
    big = buckets.filter(F.col('size') > MAX_BUCKET).withColumn('group', F.concat_ws(':', 'band', 'key'))
    members = big.select('group', F.explode('docs').alias('ocid')).join(
        features.select('ocid', 'buyer_id', 'year', 'text'), 'ocid'
    )
    # Un mismo grupo de documentos aparece en varias bandas: se identifica por su primer ocid.
    return (
        members.groupBy('group')
        .agg(
            F.count('*').alias('processes'),
            F.countDistinct('buyer_id').alias('buyers'),
            F.min('ocid').alias('first_ocid'),
            F.first('text').alias('example_text'),
            F.collect_set('year').alias('years'),
        )
        .dropDuplicates(['first_ocid', 'processes'])
        .orderBy(F.desc('processes'))
        .limit(TOP_TEMPLATES)
    )


def run_full_corpus(spark, features):
    timings = {}
    start = time.perf_counter()
    sigs = signatures(spark, features).cache()
    documents = sigs.count()
    timings['signatures_seconds'] = time.perf_counter() - start

    start = time.perf_counter()
    buckets = band_buckets(sigs).cache()
    bucket_stats = buckets.agg(
        F.count('*').alias('buckets'),
        F.sum(F.when(F.col('size') >= 2, 1).otherwise(0)).alias('shared'),
        F.sum(F.when(F.col('size') > MAX_BUCKET, 1).otherwise(0)).alias('oversized'),
        F.max('size').alias('largest'),
    ).first()
    timings['buckets_seconds'] = time.perf_counter() - start

    start = time.perf_counter()
    candidates = candidate_pairs(buckets).cache()
    candidate_count = candidates.count()
    timings['candidates_seconds'] = time.perf_counter() - start

    start = time.perf_counter()
    verified = verify(candidates, features).cache()
    similar = verified.filter(F.col('jaccard') >= THRESHOLD)
    similar_count = similar.count()
    timings['verification_seconds'] = time.perf_counter() - start

    start = time.perf_counter()
    template_groups = templates(buckets, features).toPandas()
    timings['templates_seconds'] = time.perf_counter() - start

    possible = documents * (documents - 1) // 2
    summary = dict(
        documents=documents,
        possible_pairs=possible,
        candidate_pairs=candidate_count,
        compared_fraction=candidate_count / possible if possible else 0.0,
        similar_pairs=similar_count,
        candidate_precision=similar_count / candidate_count if candidate_count else 0.0,
        hashes=HASHES,
        bands=BANDS,
        rows=ROWS,
        threshold=THRESHOLD,
        effective_threshold=effective_threshold(BANDS, ROWS),
        max_bucket=MAX_BUCKET,
        buckets=bucket_stats['buckets'],
        shared_buckets=bucket_stats['shared'],
        oversized_buckets=bucket_stats['oversized'],
        largest_bucket=bucket_stats['largest'],
        **timings,
    )
    return summary, similar, template_groups, [sigs, buckets, candidates, verified]
