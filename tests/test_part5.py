import pytest

from src.part5.rules import metrics_by_year, pick, rule_kind, targeted_rules


def test_rule_kind():
    assert rule_kind({'MODALIDAD=Licitación Pública', 'MONTO=muy alto'}) == 'normativa'
    assert rule_kind({'MODALIDAD=Subasta Inversa Electrónica', 'POSTORES=uno'}) == 'informativa'
    assert rule_kind({'DEP=sin dato', 'POSTORES=uno'}) == 'datos faltantes'


def test_targeted_rules_lift():
    # 1000 procesos; 100 con un solo postor (10 %). Entre los 50 de contratación directa, 40 tienen un postor (80 %).
    itemsets = {
        frozenset(['POSTORES=uno']): 100,
        frozenset(['MODALIDAD=Contratación Directa']): 50,
        frozenset(['MODALIDAD=Contratación Directa', 'POSTORES=uno']): 40,
        frozenset(['CAT=goods']): 600,
        frozenset(['CAT=goods', 'POSTORES=uno']): 50,
    }
    rules = targeted_rules(itemsets, 1000, 'POSTORES=uno', min_count=10, min_lift=1.2)
    assert len(rules) == 1  # CAT=goods tiene lift 0.83 y queda fuera
    rule = rules[0]
    assert rule['antecedent'] == 'MODALIDAD=Contratación Directa'
    assert rule['confidence'] == pytest.approx(0.8) and rule['lift'] == pytest.approx(8.0)


def test_metrics_by_year_recounts_each_year():
    grouped = {'Todos': [({'A', 'C'}, 3), ({'A'}, 1), ({'B'}, 4)], '2024': [({'A', 'C'}, 3), ({'B'}, 1)]}
    rows = {r['year']: r for r in metrics_by_year({'A'}, {'C'}, grouped)}
    assert rows['Todos']['confidence'] == pytest.approx(0.75) and rows['Todos']['lift'] == pytest.approx(0.75 / (3 / 8))
    assert rows['2024']['confidence'] == pytest.approx(1.0) and rows['2024']['support'] == pytest.approx(0.75)


def test_direct_contracting_single_bidder_is_normative():
    assert rule_kind({'MODALIDAD=Contratación Directa', 'DEP=LIMA', 'POSTORES=uno'}) == 'normativa'
    assert rule_kind({'MODALIDAD=Subasta Inversa Electrónica', 'DEP=LIMA', 'POSTORES=uno'}) == 'informativa'


def test_pick_skips_redundant_refinements():
    rules = [dict(antecedent='DEP=LIMA', consequent='POSTORES=uno', lift=3),
             dict(antecedent='CAT=services ∧ DEP=LIMA', consequent='POSTORES=uno', lift=2.9),
             dict(antecedent='NIVEL=local', consequent='POSTORES=uno', lift=2)]
    chosen = []
    pick(rules, 5, 'q2', chosen)
    assert [c['antecedent'] for c in chosen] == ['DEP=LIMA', 'NIVEL=local']


def test_pick_unique_items_gives_diverse_rules():
    rules = [dict(antecedent='DEP=LIMA ∧ MODALIDAD=X', consequent='POSTORES=uno', lift=5),
             dict(antecedent='CAT=goods ∧ MODALIDAD=X', consequent='POSTORES=uno', lift=4.9),
             dict(antecedent='NIVEL=local', consequent='POSTORES=uno', lift=2)]
    chosen = []
    pick(rules, 5, 'q2', chosen, unique_items=True)
    assert [c['antecedent'] for c in chosen] == ['DEP=LIMA ∧ MODALIDAD=X', 'NIVEL=local']
