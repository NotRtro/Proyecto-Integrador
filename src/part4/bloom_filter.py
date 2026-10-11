"""Bloom Filter: ¿este elemento ya apareció antes en el flujo?

Motor: Python puro (estructura de flujo).

Un arreglo de m bits y k funciones hash. Insertar enciende k bits; consultar responde
"quizá sí" si los k están encendidos y "seguro que no" si alguno está apagado. No hay
falsos negativos; la tasa de falsos positivos tras n inserciones es
(1 - e^(-k·n/m))^k, mínima con k ≈ (m/n)·ln 2.

Las k posiciones se derivan de dos hashes independientes (doble hashing mejorado,
a + i·c + i², módulo m), equivalente en la práctica a k funciones distintas.
"""

import hashlib
import math


def hashes(value, count, modulus):
    data = str(value).encode()
    a = int.from_bytes(hashlib.blake2b(data, digest_size=8, person=b'oece-a').digest(), 'little')
    c = int.from_bytes(hashlib.blake2b(data, digest_size=8, person=b'oece-b').digest(), 'little') | 1
    return [(a + i * c + i * i) % modulus for i in range(count)]


def theoretical_fpr(bits, hash_count, inserted):
    return (1 - math.exp(-hash_count * inserted / bits)) ** hash_count


def optimal_hashes(bits, inserted):
    return max(1, round(bits / max(1, inserted) * math.log(2)))


class BloomFilter:
    def __init__(self, bits, hash_count):
        if bits < 1 or hash_count < 1:
            raise ValueError('Parámetros positivos requeridos')
        self.bits, self.hash_count = bits, hash_count
        self.data = bytearray((bits + 7) // 8)

    def add(self, value):
        for p in hashes(value, self.hash_count, self.bits):
            self.data[p // 8] |= 1 << (p % 8)

    def __contains__(self, value):
        return all(self.data[p // 8] & (1 << (p % 8)) for p in hashes(value, self.hash_count, self.bits))

    def theoretical_fpr(self, inserted):
        return theoretical_fpr(self.bits, self.hash_count, inserted)

    @property
    def bytes(self):
        return len(self.data)
