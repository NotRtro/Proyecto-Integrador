"""Decodificación tolerante de una línea JSONL.

Motor: función Python pura, usada en la validación (local) y en el parseo distribuido
de Spark (`build_processes.parse`).

Algunas publicaciones del OECE han traído líneas con dos objetos JSON completos
pegados (`{...}{...}`). En vez de descartar la línea, se decodifican todos los objetos
completos; cualquier resto que no sea un objeto válido hace fallar la línea, para no
aceptar datos truncados en silencio.
"""
import json


def decode_line(line):
    """Todos los objetos JSON completos de la línea; error si hay basura o un objeto parcial."""
    decoder = json.JSONDecoder()
    position = 0
    records = []
    while position < len(line):
        while position < len(line) and line[position].isspace():
            position += 1
        if position == len(line):
            break
        record, end = decoder.raw_decode(line, position)
        if not isinstance(record, dict):
            raise ValueError('Registro no es objeto')
        records.append(record)
        position = end
    if not records:
        raise ValueError('Línea vacía')
    return records
