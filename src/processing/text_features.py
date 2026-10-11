"""Representación de cada proceso como conjunto de shingles (base de las Partes II y III).

Motor: Spark SQL, sin UDF de Python. La frecuencia documental se calcula sobre todo el
corpus con un `explode` + `groupBy`, y los shingles con funciones de orden superior
(`filter`, `transform`, `slice`), que corren dentro de la JVM.

Decisiones:
- El texto es la descripción del requerimiento y de sus ítems. El título OCDS del OECE
  es la nomenclatura del proceso (p. ej. "AS-SM-12-2024-MDCH/CS-1"): sus siglas y el año
  aparecían en más del 30 % de los documentos e inflaban la similitud entre procesos
  que no tienen nada en común. Por eso se excluye y se guarda aparte como tender_code.
- Stopwords de dominio: los términos con frecuencia documental mayor que el umbral
  (por defecto 30 %) no distinguen un requerimiento de otro ("servicio", "adquisicion",
  "distrito"...). Se calculan de los datos, no se escriben a mano, y se publican.
- Shingles de 2 palabras consecutivas: conservan el orden ("agua potable" frente a
  "potable agua") sin volverse tan específicos que dos redacciones casi iguales no
  compartan nada. Si el texto tiene una sola palabra útil, se usa esa palabra.
"""

import os

from pyspark.sql import functions as F

DOMAIN_STOPWORD_DF = float(os.getenv('DOMAIN_STOPWORD_DF', '0.30'))
SHINGLE_WIDTH = 2


def domain_stopwords(processes, threshold=DOMAIN_STOPWORD_DF):
    """Términos cuya frecuencia documental supera `threshold`, con su frecuencia."""
    total = processes.count()
    terms = (
        processes.select(F.explode(F.array_distinct(F.split('normalized_text', ' '))).alias('term'))
        .filter(F.col('term') != '')
        .groupBy('term')
        .count()
        .filter(F.col('count') > threshold * total)
        .orderBy(F.desc('count'))
        .collect()
    )
    return [dict(term=r['term'], document_frequency=r['count'] / total) for r in terms]


def shingles_column(stopwords, width=SHINGLE_WIDTH):
    """Expresión Spark: shingles ordenados y sin repetir de `normalized_text`."""
    tokens = F.filter(F.split('normalized_text', ' '), lambda t: (t != '') & ~t.isin(list(stopwords)))
    grams = F.transform(
        F.sequence(F.lit(1), F.size(tokens) - width + 1), lambda i: F.concat_ws(' ', F.slice(tokens, i, width))
    )
    return F.when(F.size(tokens) >= width, F.array_sort(F.array_distinct(grams))).otherwise(
        F.array_sort(F.array_distinct(tokens))
    )
