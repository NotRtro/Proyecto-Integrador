import json
import pytest

@pytest.mark.integration
def test_ocds_to_partitioned_parquet(tmp_path,monkeypatch):
    from src.config import spark_session
    from src.processing import build_processes as build
    source=tmp_path/'input.jsonl'
    rows=[]
    for ocid,date,release,amount in [('a','2024-01-01','2024-02-01',100),('a','2024-01-01','2025-02-01',200),('old','2014-01-01','2025-01-01',999)]:
        rows.append(dict(ocid=ocid,date=release,buyer={'id':'b','name':'Buyer'},tender={'datePublished':date,'description':'Compra de computadoras portatiles','value':{'amount':amount,'currency':'PEN'}},awards=[{'id':'award','value':{'amount':amount,'currency':'PEN'},'suppliers':[{'id':'s1'},{'id':'s2'}]}]))
    source.write_text('\n'.join(json.dumps(r) for r in rows))
    monkeypatch.setattr(build,'input_files',lambda:[source]); monkeypatch.setattr(build,'PROCESSED',tmp_path/'processed')
    monkeypatch.setattr(build,'save_json',lambda *args:None); monkeypatch.setattr(build,'save_csv',lambda *args:None)
    spark=spark_session()
    try:
        report=build.run(spark)
        assert report['selected_processes']==1 and report['duplicate_ocids']==1
        df=spark.read.parquet(str(tmp_path/'processed'/'processes'))
        row=df.first(); assert row.ocid=='a' and row.tender_amount_pen==200
        suppliers=spark.read.parquet(str(tmp_path/'processed'/'suppliers')).collect()
        assert len(suppliers)==2 and sum(r.allocated_amount_pen for r in suppliers)==200
    finally: spark.stop()
