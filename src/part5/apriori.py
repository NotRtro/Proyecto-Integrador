"""Apriori propio: conjuntos frecuentes por niveles.

Motor: Python puro sobre una muestra (en el driver). Sirve de referencia: FP-Growth de
Spark debe encontrar exactamente los mismos conjuntos sobre las mismas canastas.

Nivel k: candidatos = uniones de dos conjuntos frecuentes de tamaño k-1 que dan tamaño
k y cuyos subconjuntos de tamaño k-1 son todos frecuentes (propiedad Apriori: si un
conjunto es infrecuente, ningún superconjunto puede ser frecuente). Luego se cuenta el
soporte de cada candidato recorriendo las canastas.
"""

import itertools
import math
from collections import Counter


def apriori(transactions, min_support=0.02):
    """Diccionario {conjunto frecuente: frecuencia absoluta}."""
    baskets = [set(t) for t in transactions]
    n = len(baskets)
    if not n:
        return {}
    minimum = math.ceil(min_support * n)
    counts = Counter(frozenset([item]) for basket in baskets for item in basket)
    level = {itemset: count for itemset, count in counts.items() if count >= minimum}
    frequent = dict(level)
    size = 2
    while level:
        previous = set(level)
        keys = list(previous)
        candidates = {
            a | b
            for i, a in enumerate(keys)
            for b in keys[i + 1 :]
            if len(a | b) == size and all(frozenset(s) in previous for s in itertools.combinations(a | b, size - 1))
        }
        level = {c: sum(c <= basket for basket in baskets) for c in candidates}
        level = {c: count for c, count in level.items() if count >= minimum}
        frequent.update(level)
        size += 1
    return frequent
