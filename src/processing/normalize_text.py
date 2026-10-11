"""Normalización de texto en español para la representación de los procesos.

Motor: función Python pura (se ejecuta dentro del parseo distribuido de Spark).

Pasos: quitar tildes (NFKD -> ASCII), minúsculas, tokens alfanuméricos, y descartar
artículos/preposiciones y tokens de un carácter. Las stopwords propias del dominio
(palabras que aparecen en casi todos los procesos) no se fijan aquí: se calculan con
los datos en src/processing/text_features.py.
"""
import re
import unicodedata

STOP = set('de del la las el los y o en para por con un una al a se que su'.split())


def normalize_text(text):
    text = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode().lower()
    return ' '.join(t for t in re.findall(r'[a-z0-9]+', text) if t not in STOP and len(t) > 1)
