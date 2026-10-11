import random

from src.part2.exact_jaccard import jaccard
from src.part4.bloom_filter import BloomFilter
from src.part4.count_min_sketch import CountMinSketch
from src.part4.dgim import DGIM
from src.part5.apriori import apriori
from src.part5.transactions import transaction
from src.processing.model import amount_pen, converted_amount, effective_date, flatten
from src.processing.normalize_text import normalize_text


def test_normalization():
    assert normalize_text('Adquisición de ÚTILES, para la oficina') == 'adquisicion utiles oficina'
    assert normalize_text('a') == ''


def test_text_excludes_tender_code():
    # El título es la nomenclatura del proceso: va a tender_code, no al texto de similitud.
    record = {
        'ocid': 'x',
        'tender': {
            'title': 'AS-SM-12-2024-MDCH/CS-1',
            'description': 'Compra de cemento',
            'items': [{'description': 'Cemento portland'}],
        },
    }
    process = flatten(record, 'f')['processes'][0]
    assert process['tender_code'] == 'AS-SM-12-2024-MDCH/CS-1'
    assert (
        'sm' not in process['normalized_text'].split()
        and process['normalized_text'] == 'compra cemento cemento portland'
    )


def test_dates_and_currency():
    assert (
        effective_date({'date': '2025-01-01', 'tender': {'datePublished': '2014-01-30T19:07:00-05:00'}})[0]
        == '2014-01-30'
    )
    assert (
        effective_date({'tender': {'datePublished': 'bad', 'tenderPeriod': {'startDate': '2024-01-01'}}})[0]
        == '2024-01-01'
    )
    assert effective_date({'date': '2025-01-01'})[0] is None
    assert amount_pen({'amount': 12, 'currency': 'USD'}) is None
    assert amount_pen({'amount': 12, 'currency': 'USD', 'amount_PEN': 45}) == 45


def test_supplier_allocation():
    r = {
        'ocid': 'one',
        'tender': {'datePublished': '2024-01-01'},
        'awards': [{'id': 'a', 'value': {'amount': 100, 'currency': 'PEN'}, 'suppliers': [{'id': 's1'}, {'id': 's2'}]}],
    }
    flat = flatten(r, 'source')
    assert sum(s['allocated_amount_pen'] for s in flat['suppliers']) == 100
    assert flat['processes'][0]['award_amount_pen'] == 100


def test_foreign_award_uses_auditable_tender_rate():
    r = {
        'ocid': 'usd',
        'tender': {'datePublished': '2024-01-01', 'value': {'amount': 100, 'amount_PEN': 375, 'currency': 'USD'}},
        'awards': [{'id': 'a', 'value': {'amount': 80, 'currency': 'USD'}, 'suppliers': [{'id': 's'}]}],
    }
    flat = flatten(r, 'source')
    award = flat['awards'][0]
    process = flat['processes'][0]
    assert award['amount_pen'] == 300
    assert award['conversion_method'] == 'tender_implied_rate'
    assert award['exchange_rate'] == 3.75
    assert process['award_amount_pen'] == 300 and process['award_converted_count'] == 1
    assert flat['suppliers'][0]['allocated_amount_pen'] == 300
    assert converted_amount({'amount': 80, 'currency': 'EUR'}, 'USD', 3.75)[0] is None


def test_similarity():
    assert jaccard({'a', 'b'}, {'b', 'c'}) == 1 / 3
    assert jaccard([], []) == 0


def test_bloom_cms():
    b = BloomFilter(1000, 4)
    c = CountMinSketch(20, 3)
    values = [str(i % 57) for i in range(1000)]
    for v in values:
        b.add(v)
        c.add(v)
    assert all(v in b for v in values)
    assert all(c.query(v) >= values.count(v) for v in set(values))


def test_dgim_bounds():
    rng = random.Random(42)
    for window in [1, 2, 10, 100]:
        for probability in [0, 0.01, 0.5, 1]:
            d = DGIM(window)
            history = []
            for _ in range(1000):
                bit = rng.random() < probability
                history.append(bit)
                d.add(bit)
                exact = sum(history[-window:])
                assert abs(d.query() - exact) <= max(0.5, exact * 0.5)
                assert all(sum(s == size for s, _ in d.buckets) <= 2 for size, _ in d.buckets)


def test_transactions_apriori():
    t = transaction(
        dict(
            year=2024,
            category='goods',
            modality='Licitación Pública',
            department=None,
            amount_band='alto',
            competition='uno',
            buyer_level=float('nan'),
        )
    )
    assert len(t) == 6 and not any(i.startswith('ANIO=') for i in t)
    assert 'MODALIDAD=Licitación Pública' in t and 'DEP=sin dato' in t and 'NIVEL=sin dato' in t
    result = apriori([['a', 'b'], ['a'], ['b', 'c']], 0.5)
    assert result == {frozenset(['a']): 2, frozenset(['b']): 2}


def test_concatenated_json_recovery():
    import pytest

    from src.ingestion.json_records import decode_line

    assert decode_line('{"ocid":"a"}{"ocid":"b"}\n') == [{'ocid': 'a'}, {'ocid': 'b'}]
    with pytest.raises(ValueError):
        decode_line('{"ocid":"a"}garbage')


def test_procurement_modality_groups_variants():
    from src.processing.model import procurement_modality

    assert procurement_modality('Adjudicación Simplificada - Ley N° 31728') == 'Adjudicación Simplificada'
    assert procurement_modality('Licitación Pública Abreviada Homologación') == 'Licitación Pública Abreviada'
    assert procurement_modality('Licitación Pública') == 'Licitación Pública'
    assert procurement_modality('Comparación de Precios') == 'Comparación de Precios'
    assert procurement_modality('Contratos Estandarizados') == 'Otra' and procurement_modality(None) is None


def test_experiment_registry_reuses_or_measures(tmp_path, monkeypatch):
    from src import common

    monkeypatch.setattr(common, 'ARTIFACTS', tmp_path)
    calls = []

    def measure():
        calls.append(1)
        (tmp_path / 'x.csv').write_text('a\n1\n')

    monkeypatch.setenv('RUN_EXPERIMENTS', 'auto')
    assert common.experiment('demo', ['x.csv'], measure) and len(calls) == 1  # falta: se mide
    assert not common.experiment('demo', ['x.csv'], measure) and len(calls) == 1  # existe: se reutiliza
    monkeypatch.setenv('RUN_EXPERIMENTS', 'si')
    assert common.experiment('demo', ['x.csv'], measure) and len(calls) == 2
    import json

    registry = json.loads((tmp_path / 'metadata/experiments.json').read_text(encoding='utf-8'))
    assert registry['demo']['last_status'] == 'medido' and registry['demo']['profile'] == 'completo'


def test_tenderers_are_unique_per_process():
    r = {
        'ocid': 't',
        'tender': {
            'datePublished': '2024-01-01',
            'tenderers': [{'id': 'A', 'name': 'a'}, {'id': 'A', 'name': 'a'}, {'id': 'B', 'name': 'b'}],
        },
        'buyer': {'id': 'b1'},
    }
    rows = flatten(r, 'source')['tenderers']
    assert sorted(x['tenderer_id'] for x in rows) == ['A', 'B'] and all(x['buyer_id'] == 'b1' for x in rows)
