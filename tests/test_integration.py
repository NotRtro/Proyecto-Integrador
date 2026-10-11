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
    rows.append(dict(ocid='usd',date='2024-02-02',buyer={'id':'b2','name':'Buyer USD'},tender={'datePublished':'2024-02-01','value':{'amount':100,'amount_PEN':380,'currency':'USD'}},awards=[{'id':'award-usd','value':{'amount':50,'currency':'USD'},'suppliers':[{'id':'s3'}]}]))
    rows.append(dict(ocid='usd-zero',date='2024-02-02',buyer={'id':'b3','name':'Buyer USD zero'},tender={'datePublished':'2024-02-01','value':{'amount':0,'amount_PEN':0,'currency':'USD'}},awards=[{'id':'award-usd-zero','date':'2024-02-01','value':{'amount':10,'currency':'USD'},'suppliers':[{'id':'s4'}]}]))
    source.write_text('\n'.join(json.dumps(r) for r in rows))
    monkeypatch.setattr(build,'input_files',lambda:[source]); monkeypatch.setattr(build,'PROCESSED',tmp_path/'processed')
    monkeypatch.setattr(build,'save_json',lambda *args:None); monkeypatch.setattr(build,'save_csv',lambda *args:None)
    spark=spark_session()
    try:
        report=build.run(spark)
        assert report['selected_processes']==3 and report['duplicate_ocids']==1
        df=spark.read.parquet(str(tmp_path/'processed'/'processes'))
        row=df.filter("ocid = 'a'").first(); assert row.tender_amount_pen==200
        usd=df.filter("ocid = 'usd'").first(); assert usd.award_amount_pen==190 and usd.award_converted_count==1
        usd_zero=df.filter("ocid = 'usd-zero'").first(); assert usd_zero.award_amount_pen==38 and usd_zero.award_unconverted_count==0
        # La conversión histórica debe registrar método y tasa, no solo el monto.
        award=spark.read.parquet(str(tmp_path/'processed'/'awards')).filter("ocid = 'usd-zero'").first()
        assert award.conversion_method=='oece_daily_median_rate' and award.exchange_rate==3.8 and award.amount_pen==38
        suppliers=spark.read.parquet(str(tmp_path/'processed'/'suppliers')).collect()
        assert len(suppliers)==4 and sum(r.allocated_amount_pen for r in suppliers)==428
    finally: spark.stop()
