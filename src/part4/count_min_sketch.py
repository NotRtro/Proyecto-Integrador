"""Count-Min Sketch: frecuencia aproximada de cada elemento del flujo.

Motor: Python puro (estructura de flujo).

Una tabla de d filas x w contadores, cada fila con su función hash. Insertar suma 1 en
una celda por fila; la frecuencia estimada es el mínimo de esas d celdas. Nunca
subestima (las colisiones solo suman). Garantía: con probabilidad al menos 1 - δ,
estimado - exacto <= ε·N, con ε = e/w y δ = e^(-d), donde N es el total de eventos.

Es útil cuando hay muchos elementos distintos (decenas de miles de proveedores): un
diccionario exacto crece con la cardinalidad, el sketch ocupa w·d contadores fijos.
"""
import hashlib
import math
from array import array


def epsilon(width):
    return math.e / width


def delta(depth):
    return math.exp(-depth)


class CountMinSketch:
    def __init__(self, width, depth):
        if width < 1 or depth < 1:
            raise ValueError('Parámetros positivos requeridos')
        self.width, self.depth = width, depth
        self.data = [array('Q', [0]) * width for _ in range(depth)]
        self.total = 0

    def _positions(self, value):
        data = str(value).encode()
        return [int.from_bytes(hashlib.blake2b(data, digest_size=8, person=f'cms{i}'.encode()).digest(), 'little')
                % self.width for i in range(self.depth)]

    def add(self, value):
        self.total += 1
        for row, p in zip(self.data, self._positions(value)):
            row[p] += 1

    def query(self, value):
        return min(row[p] for row, p in zip(self.data, self._positions(value)))

    @property
    def bytes(self):
        return self.width * self.depth * 8
