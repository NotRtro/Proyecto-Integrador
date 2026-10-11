"""Reglas de asociación: métricas, tipo de regla y reglas dirigidas a un consecuente.

Motor: Python sobre los conjuntos frecuentes que entrega FP-Growth (Spark). Los
conjuntos ya están agregados (miles de filas), así que derivar reglas es barato.

Métricas de A → C sobre n canastas:
    soporte = frec(A ∪ C) / n
    confianza = frec(A ∪ C) / frec(A)
    lift = confianza / (frec(C) / n)    (> 1: A y C aparecen juntos más que por azar)

Tipo de regla, para no presentar como hallazgo lo que dicta la ley:
- normativa: la Ley de Contrataciones fija la modalidad según el objeto (CAT) y el valor
  estimado (MONTO), p. ej. obras de monto alto -> licitación. Es normativa una regla que
  solo relaciona esos tres atributos, o que predice la modalidad a partir del objeto o
  del monto (o al revés), aunque traiga otros ítems: "MONTO=bajo ∧ NIVEL=x →
  MODALIDAD=Comparación de Precios" redescubre el umbral legal. También lo es
  "Contratación Directa ↔ un postor": la contratación directa se hace con un único
  proveedor por definición.
- datos faltantes: incluye "sin dato"; describe la calidad del registro, no la compra.
- informativa: el resto.

Improvement (Bayardo, Agrawal y Gunopulos, 2000): confianza de A → C menos la mejor
confianza de A' → C con A' ⊂ A (incluido A' = ∅, cuya confianza es la tasa base). Una
regla con improvement ≈ 0 no aporta nada sobre una regla más simple: "CP ∧ NIVEL=local →
dos a tres postores" (90 %) solo repite "CP → dos a tres postores" (88 %).
"""

from itertools import combinations

from src.part5.transactions import MISSING, attribute

NORMATIVE_ATTRIBUTES = {'MODALIDAD', 'MONTO', 'CAT'}
# Determinantes legales de la modalidad: objeto de la contratación y valor estimado.
MODALITY_DETERMINANTS = {'CAT', 'MONTO'}
NORMATIVE_PAIRS = [{'MODALIDAD=Contratación Directa', 'POSTORES=uno'}]


def rule_kind(items, consequent=None):
    """Tipo de la regla con ítems `items` (antecedente ∪ consecuente).

    Con `consequent` (un ítem) se reconoce además la dirección: predecir la modalidad a
    partir del objeto o del monto, o el objeto o el monto a partir de la modalidad.
    """
    items = set(items)
    if any(item.endswith(f'={MISSING}') for item in items):
        return 'datos faltantes'
    if {attribute(item) for item in items} <= NORMATIVE_ATTRIBUTES or any(pair <= items for pair in NORMATIVE_PAIRS):
        return 'normativa'
    if consequent is not None:
        target = attribute(consequent)
        others = {attribute(item) for item in items - {consequent}}
        if (target == 'MODALIDAD' and others & MODALITY_DETERMINANTS) or (
            target in MODALITY_DETERMINANTS and 'MODALIDAD' in others
        ):
            return 'normativa'
    return 'informativa'


def improvement(antecedent, consequent, frequencies, n):
    """Confianza de A → C menos la mejor confianza de sus sub-reglas A' → C (A' ⊂ A).

    frequencies: {frozenset: frecuencia} con todos los subconjuntos necesarios (si A ∪ C
    es frecuente, todos sus subconjuntos también lo son, así que FP-Growth los trae).
    """
    antecedent = frozenset(antecedent)
    consequent = frozenset([consequent])

    def confidence(a):
        if not a:
            return frequencies[consequent] / n
        return frequencies[a | consequent] / frequencies[a]

    best_sub = max(
        confidence(frozenset(sub)) for size in range(len(antecedent)) for sub in combinations(antecedent, size)
    )
    return confidence(antecedent) - best_sub


def targeted_rules(itemsets, n, consequent, min_count, min_lift, max_antecedent=3):
    """Reglas A → consequent a partir de conjuntos frecuentes {frozenset: frecuencia}.

    Se usan cuando interesa un resultado concreto (un solo postor) aunque su confianza
    sea baja: lo relevante es cuánto sube su probabilidad (lift) dentro del grupo A.
    """
    base = itemsets.get(frozenset([consequent]))
    if not base:
        return []
    base_rate = base / n
    rules = []
    for itemset, count in itemsets.items():
        if consequent not in itemset or len(itemset) < 2 or count < min_count:
            continue
        antecedent = itemset - {consequent}
        if len(antecedent) > max_antecedent or any(attribute(i) == attribute(consequent) for i in antecedent):
            continue
        antecedent_count = itemsets.get(antecedent)
        if not antecedent_count:
            continue
        confidence = count / antecedent_count
        lift = confidence / base_rate
        if lift >= min_lift:
            gain = improvement(antecedent, consequent, itemsets, n)
            rules.append(
                dict(
                    antecedent=' ∧ '.join(sorted(antecedent)),
                    consequent=consequent,
                    count=count,
                    antecedent_count=antecedent_count,
                    support=count / n,
                    confidence=confidence,
                    lift=lift,
                    base_rate=base_rate,
                    size=len(itemset),
                    improvement=gain,
                    kind=rule_kind(itemset, consequent),
                )
            )
    return sorted(rules, key=lambda r: (-r['lift'], -r['count']))


def metrics_by_year(antecedent, consequent, grouped):
    """Soporte, confianza y lift de una regla recalculados sobre las canastas de cada año.

    grouped: {año: [(conjunto de ítems, número de canastas)]}, con 'Todos' incluido.
    """
    rows = []
    for year, entries in grouped.items():
        total = sum(c for _, c in entries)
        joint = sum(c for basket, c in entries if antecedent | consequent <= basket)
        with_antecedent = sum(c for basket, c in entries if antecedent <= basket)
        with_consequent = sum(c for basket, c in entries if consequent <= basket)
        confidence = joint / with_antecedent if with_antecedent else 0.0
        lift = confidence / (with_consequent / total) if with_consequent else 0.0
        rows.append(
            dict(
                year=year,
                support=joint / total if total else 0.0,
                confidence=confidence,
                lift=lift,
                count=joint,
                transactions=total,
            )
        )
    return rows


def pick(candidates, limit, source, chosen, unique_items=False):
    """Agrega hasta `limit` reglas sin repetir ni refinar una ya elegida.

    Una regla es redundante si usa los mismos ítems que otra elegida, o si tiene el mismo
    consecuente y su antecedente contiene (o está contenido en) el de otra elegida:
    "LIMA ∧ servicios → X" no aporta mucho si ya está "LIMA → X". Con unique_items, además
    ningún ítem del antecedente se repite entre las reglas elegidas de esta fuente, para
    que no salgan cinco variantes del mismo factor.
    """
    added = 0
    for rule in candidates:
        antecedent = frozenset(rule['antecedent'].split(' ∧ '))
        items = antecedent | {rule['consequent']}
        redundant = any(
            items == c['_items']
            or (
                rule['consequent'] == c['consequent']
                and (antecedent <= c['_antecedent'] or c['_antecedent'] <= antecedent)
            )
            for c in chosen
        )
        if unique_items:
            redundant = redundant or any(antecedent & c['_antecedent'] for c in chosen if c['source'] == source)
        if not redundant:
            chosen.append(dict(rule, source=source, _items=items, _antecedent=antecedent))
            added += 1
        if added == limit:
            break


def automatic_reading(rule):
    """Lectura literal de las métricas. No es una interpretación: esa la escribe el equipo."""
    return (
        f"Entre los procesos con {rule['antecedent']}, el {rule['confidence']:.1%} tiene {rule['consequent']} "
        f"(soporte {rule['support']:.1%}, lift {rule['lift']:.2f})."
    )
