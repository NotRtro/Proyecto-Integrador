"""MinHash y bandas LSH (implementación propia).

Motor: funciones numpy puras. Se ejecutan en el driver para la muestra de evaluación y
dentro de `mapPartitions` en los ejecutores de Spark para el corpus completo.

MinHash: para cada función hash h_i(x) = (a_i·x + b_i) mod p, la firma guarda el mínimo
sobre los shingles del documento. P[min h(A) = min h(B)] = Jaccard(A, B), así que la
proporción de componentes iguales entre dos firmas estima Jaccard con error estándar
sqrt(J(1-J)/k) para k funciones.

Se usa el primo de Mersenne p = 2^31 - 1: con a, x < p el producto a·x < 2^62 cabe en
uint64 sin desbordar, lo que permite vectorizar con numpy (k funciones x t shingles).

LSH: la firma se divide en b bandas de r filas; dos documentos son candidatos si
coinciden en todas las filas de alguna banda. P[candidato] = 1 - (1 - s^r)^b, una curva
S con umbral aproximado (1/b)^(1/r).
"""

import hashlib

import numpy as np

MERSENNE = (1 << 31) - 1


def coefficients(hashes, seed):
    """Coeficientes (a, b) de las funciones hash; mismos para todos los documentos."""
    rng = np.random.default_rng(seed)
    return (
        rng.integers(1, MERSENNE, size=hashes, dtype=np.uint64),
        rng.integers(0, MERSENNE, size=hashes, dtype=np.uint64),
    )


def token_hashes(tokens):
    """Hash estable (independiente del proceso de Python) de cada shingle, módulo p."""
    return np.fromiter(
        (int.from_bytes(hashlib.blake2b(t.encode(), digest_size=8).digest(), 'little') % MERSENNE for t in tokens),
        dtype=np.uint64,
        count=len(tokens),
    )


def signature(tokens, hashes=128, seed=42, coeffs=None):
    """Firma MinHash de un conjunto de shingles (vector de `hashes` enteros)."""
    a, b = coeffs if coeffs is not None else coefficients(hashes, seed)
    if len(tokens) == 0:
        return np.full(len(a), MERSENNE, dtype=np.uint64)
    x = token_hashes(sorted(set(tokens)))
    return ((a[:, None] * x[None, :] + b[:, None]) % MERSENNE).min(axis=1)


def estimate(sig_a, sig_b, k=None):
    """Jaccard estimado con los primeros k componentes de las firmas."""
    k = k or len(sig_a)
    return float(np.mean(sig_a[:k] == sig_b[:k]))


def band_keys(sig, bands, rows):
    """Clave (banda, hash de sus r componentes) por cada banda de la firma."""
    return [
        (band, hashlib.blake2b(sig[band * rows : (band + 1) * rows].tobytes(), digest_size=8).hexdigest())
        for band in range(bands)
    ]


def candidate_probability(similarity, bands, rows):
    """Probabilidad teórica de que un par con Jaccard `similarity` sea candidato."""
    return 1 - (1 - np.asarray(similarity, dtype=float) ** rows) ** bands


def effective_threshold(bands, rows):
    """Similitud donde la curva S sube más rápido (aproximación (1/b)^(1/r))."""
    return (1 / bands) ** (1 / rows)
