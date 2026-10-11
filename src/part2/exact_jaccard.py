"""Jaccard exacto entre dos conjuntos: la referencia de toda la Parte II.

Motor: Python puro. evaluation.py calcula lo mismo para todos los pares de la muestra
con un producto de matriz dispersa; esta función es la definición directa y se usa en
los tests para comprobar que ambas coinciden.
"""


def jaccard(a, b):
    """|A ∩ B| / |A ∪ B|; 0 si ambos conjuntos están vacíos."""
    a, b = set(a), set(b)
    return len(a & b) / len(a | b) if a | b else 0.0
