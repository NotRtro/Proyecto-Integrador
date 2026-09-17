import random
from src.processing.normalize_text import normalize_text,shingles
from src.processing.model import effective_date,amount_pen,flatten
from src.part2.exact_jaccard import jaccard
from src.part2.minhash_lsh import signature
from src.part4.bloom_filter import BloomFilter
from src.part4.count_min_sketch import CountMinSketch
from src.part4.dgim import DGIM
from src.part5.transactions import transaction
from src.part5.apriori import apriori

def test_normalization():
    assert normalize_text('Adquisición de ÚTILES, para la oficina')=='adquisicion utiles oficina'
    assert shingles('a')==[]

def test_dates_and_currency():
    assert effective_date({'date':'2025-01-01','tender':{'datePublished':'2014-01-30T19:07:00-05:00'}})[0]=='2014-01-30'
    assert effective_date({'tender':{'datePublished':'bad','tenderPeriod':{'startDate':'2024-01-01'}}})[0]=='2024-01-01'
    assert effective_date({'date':'2025-01-01'})[0] is None
    assert amount_pen({'amount':12,'currency':'USD'}) is None
    assert amount_pen({'amount':12,'currency':'USD','amount_PEN':45})==45

def test_supplier_allocation():
    r={'ocid':'one','tender':{'datePublished':'2024-01-01'},'awards':[{'id':'a','value':{'amount':100,'currency':'PEN'},'suppliers':[{'id':'s1'},{'id':'s2'}]}]}
    flat=flatten(r,'source'); assert sum(s['allocated_amount_pen'] for s in flat['suppliers'])==100
    assert flat['processes'][0]['award_amount_pen']==100

def test_similarity():
    assert jaccard({'a','b'},{'b','c'})==1/3
    assert jaccard([],[])==0
    assert signature(['a','b'])==signature(['b','a'])
    assert signature(['a'],32,1)!=signature(['a'],32,2)

def test_bloom_cms():
    b=BloomFilter(1000,4); c=CountMinSketch(20,3)
    values=[str(i%57) for i in range(1000)]
    for v in values: b.add(v); c.add(v)
    assert all(v in b for v in values)
    assert all(c.query(v)>=values.count(v) for v in set(values))

def test_dgim_bounds():
    rng=random.Random(42)
    for window in [1,2,10,100]:
        for probability in [0,.01,.5,1]:
            d=DGIM(window); history=[]
            for _ in range(1000):
                bit=rng.random()<probability; history.append(bit); d.add(bit)
                exact=sum(history[-window:])
                assert abs(d.query()-exact)<=max(.5,exact*.5)
                assert all(sum(s==size for s,_ in d.buckets)<=2 for size,_ in d.buckets)

def test_transactions_apriori():
    t=transaction(dict(year=2024,category='goods',method='open',department='LIMA',amount_band='alto',competition='uno'))
    assert len(t)==6 and 'ANIO=2024' in t
    result=apriori([['a','b'],['a'],['b','c']],.5)
    assert result=={frozenset(['a']):2,frozenset(['b']):2}


def test_concatenated_json_recovery():
    import pytest
    from src.ingestion.json_records import decode_line
    assert decode_line('{"ocid":"a"}{"ocid":"b"}\n')==[{'ocid':'a'},{'ocid':'b'}]
    with pytest.raises(ValueError): decode_line('{"ocid":"a"}garbage')
