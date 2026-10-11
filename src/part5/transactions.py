"""Cada proceso como una canasta de características (Parte V).

Motor: función Python pura (se aplica igual en Spark y en la muestra local).

Ítems de la canasta, uno por atributo, con el prefijo del atributo para que la regla se
lea sola ("MODALIDAD=Contratación Directa → POSTORES=uno"):
- CAT: bienes, servicios u obras (mainProcurementCategory).
- MODALIDAD: modalidad peruana agrupada (procurementMethodDetails).
- DEP: departamento del comprador.
- MONTO: banda del monto convocado (bajo < 100 mil, medio < 1 M, alto < 10 M, muy alto).
- POSTORES: uno, dos a tres, cuatro o más.
- NIVEL: nivel de gobierno del comprador.

El año queda fuera de la canasta: no es una característica del proceso sino el
momento, y producía reglas cronológicas obvias. Las métricas por año se calculan
aparte, recontando cada regla sobre las canastas de ese año.
"""

MISSING = 'sin dato'
ATTRIBUTES = [
    ('CAT', 'category'),
    ('MODALIDAD', 'modality'),
    ('DEP', 'department'),
    ('MONTO', 'amount_band'),
    ('POSTORES', 'competition'),
    ('NIVEL', 'buyer_level'),
]


def transaction(row):
    def value(column):
        raw = row[column]
        # raw != raw detecta NaN, que es como pandas representa un faltante.
        return MISSING if raw is None or raw != raw or raw in ('', 'NO INFORMADO') else raw

    return sorted(f'{prefix}={value(column)}' for prefix, column in ATTRIBUTES)


def attribute(item):
    return item.split('=', 1)[0]
