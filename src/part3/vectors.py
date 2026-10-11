"""Representación vectorial de los procesos para búsqueda de vecinos (Parte III).

Motor: local (scikit-learn en el driver). El corpus completo (~196 mil textos) cabe en
memoria como matriz dispersa y TF-IDF + SVD truncada es un cálculo de una sola máquina;
Spark no lo acelera aquí.

Decisiones:
- TF-IDF de unigramas y bigramas sobre el mismo texto normalizado de la Parte II (sin
  nomenclatura). A diferencia de los shingles binarios, pondera: una palabra rara
  ("cistoresectoscopio") pesa más que una frecuente ("mantenimiento"), que es lo que
  importa para ordenar vecinos y no solo para decidir si dos textos se solapan.
- SVD truncada a 256 dimensiones (análisis semántico latente): vectores densos y
  pequeños (196 mil x 256 x 4 bytes ≈ 200 MB) sobre los que operan tanto la búsqueda
  exacta como HNSW e IVF. Todas las comparaciones usan exactamente los mismos vectores.
- Normalización L2: el producto interno es la similitud coseno.
"""

import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from src.config import SEED

MAX_FEATURES = 50_000
DIMENSIONS = 256
MIN_DF = 3


def build_vectors(texts):
    """Matriz densa float32 normalizada (documentos x DIMENSIONS) y métricas de la reducción."""
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2), max_features=MAX_FEATURES, min_df=MIN_DF, sublinear_tf=True, dtype=np.float32
    )
    tfidf = vectorizer.fit_transform(texts)
    svd = TruncatedSVD(n_components=DIMENSIONS, random_state=SEED, algorithm='randomized', n_iter=5)
    reduced = normalize(svd.fit_transform(tfidf)).astype(np.float32)
    info = dict(
        documents=tfidf.shape[0],
        vocabulary=len(vectorizer.vocabulary_),
        dimensions=DIMENSIONS,
        explained_variance=float(svd.explained_variance_ratio_.sum()),
        tfidf_nonzeros=int(tfidf.nnz),
        ngram_range=[1, 2],
        min_df=MIN_DF,
        sublinear_tf=True,
        max_features=MAX_FEATURES,
    )
    return np.ascontiguousarray(reduced), info
