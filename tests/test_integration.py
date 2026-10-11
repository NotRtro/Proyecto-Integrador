import json

import pytest


@pytest.mark.integration
def test_ocds_to_partitioned_parquet(tmp_path, monkeypatch):
    from src.config import spark_session
    from src.processing import build_processes as build

    source = tmp_path / 'input.jsonl'
    rows = []
    for ocid, date, release, amount in [
        ('a', '2024-01-01', '2024-02-01', 100),
        ('a', '2024-01-01', '2025-02-01', 200),
        ('old', '2014-01-01', '2025-01-01', 999),
    ]:
        rows.append(
            dict(
                ocid=ocid,
                date=release,
                buyer={'id': 'b', 'name': 'Buyer'},
                tender={
                    'datePublished': date,
                    'description': 'Compra de computadoras portatiles',
                    'value': {'amount': amount, 'currency': 'PEN'},
                },
                awards=[
                    {
                        'id': 'award',
                        'value': {'amount': amount, 'currency': 'PEN'},
                        'suppliers': [{'id': 's1'}, {'id': 's2'}],
                    }
                ],
            )
        )
    rows.append(
        dict(
            ocid='usd',
            date='2024-02-02',
            buyer={'id': 'b2', 'name': 'Buyer USD'},
            tender={'datePublished': '2024-02-01', 'value': {'amount': 100, 'amount_PEN': 380, 'currency': 'USD'}},
            awards=[{'id': 'award-usd', 'value': {'amount': 50, 'currency': 'USD'}, 'suppliers': [{'id': 's3'}]}],
        )
    )
    rows.append(
        dict(
            ocid='usd-zero',
            date='2024-02-02',
            buyer={'id': 'b3', 'name': 'Buyer USD zero'},
            tender={'datePublished': '2024-02-01', 'value': {'amount': 0, 'amount_PEN': 0, 'currency': 'USD'}},
            awards=[
                {
                    'id': 'award-usd-zero',
                    'date': '2024-02-01',
                    'value': {'amount': 10, 'currency': 'USD'},
                    'suppliers': [{'id': 's4'}],
                }
            ],
        )
    )
    source.write_text('\n'.join(json.dumps(r) for r in rows))
    monkeypatch.setattr(build, 'input_files', lambda: [source])
    monkeypatch.setattr(build, 'PROCESSED', tmp_path / 'processed')
    monkeypatch.setattr(build, 'save_json', lambda *args: None)
    monkeypatch.setattr(build, 'save_csv', lambda *args: None)
    spark = spark_session()
    try:
        report = build.run(spark)
        assert report['selected_processes'] == 3 and report['duplicate_ocids'] == 1
        df = spark.read.parquet(str(tmp_path / 'processed' / 'processes'))
        row = df.filter("ocid = 'a'").first()
        assert row.tender_amount_pen == 200
        # process_features es la tabla delgada para similitud y reglas; processes no lleva shingles.
        features = spark.read.parquet(str(tmp_path / 'processed' / 'process_features'))
        assert set(features.columns) == set(build.FEATURE_COLUMNS) and 'shingles' not in df.columns
        # Con 3 documentos cada término supera df 30 % y se vuelve stopword: el caso real se prueba
        # en test_domain_stopwords_and_shingles; aquí solo se verifica el tipo de la columna.
        assert isinstance(features.filter("ocid = 'a'").first().shingles, list)
        assert row.buyer_level == 'otro' and report['buyer_levels']['classified_share'] == 0.0
        usd = df.filter("ocid = 'usd'").first()
        assert usd.award_amount_pen == 190 and usd.award_converted_count == 1
        usd_zero = df.filter("ocid = 'usd-zero'").first()
        assert usd_zero.award_amount_pen == 38 and usd_zero.award_unconverted_count == 0
        # La conversión histórica debe registrar método y tasa, no solo el monto.
        award = spark.read.parquet(str(tmp_path / 'processed' / 'awards')).filter("ocid = 'usd-zero'").first()
        assert (
            award.conversion_method == 'oece_daily_median_rate'
            and award.exchange_rate == 3.8
            and award.amount_pen == 38
        )
        suppliers = spark.read.parquet(str(tmp_path / 'processed' / 'suppliers')).collect()
        assert len(suppliers) == 4 and sum(r.allocated_amount_pen for r in suppliers) == 428
    finally:
        spark.stop()


@pytest.mark.integration
def test_domain_stopwords_and_shingles():
    from src.config import spark_session
    from src.processing.text_features import domain_stopwords, shingles_column

    spark = spark_session()
    try:
        texts = ['servicio limpieza local', 'servicio agua potable', 'servicio agua potable rural', 'cemento']
        df = spark.createDataFrame([(t,) for t in texts], 'normalized_text string')
        stop = domain_stopwords(df, 0.5)
        # "servicio" aparece en 3 de 4 documentos (75 % > 50 %); "agua" en 2 de 4 (no supera).
        assert [t['term'] for t in stop] == ['servicio'] and stop[0]['document_frequency'] == 0.75
        out = [r.s for r in df.select(shingles_column(['servicio']).alias('s')).collect()]
        assert out[0] == ['limpieza local']
        assert out[2] == ['agua potable', 'potable rural']
        assert out[3] == ['cemento']
    finally:
        spark.stop()


@pytest.mark.integration
def test_normalize_suppliers_canonical_name_and_type():
    from src.config import spark_session
    from src.processing import build_processes as build

    spark = spark_session()
    try:
        rows = [
            ('a', '2024-01-01', 'PE-RUC-10123456789', 'ARRIETA NINO LUIS'),
            ('b', '2025-01-01', 'PE-RUC-10123456789', 'ARRIETA NIÑO LUIS'),
            ('c', '2024-01-01', 'PE-RUC-20123456789', 'EMPRESA X S.A.C.'),
            ('d', '2024-01-01', 'PE-RUC-555574', 'CONSORCIO CARMADI - DMV E.I.R.L.'),
            ('e', '2024-01-01', 'PE-RUC-1704037', 'ROLOPOM CORPORIS II'),
            ('f', '2024-01-01', 'PE-RUC-L0609016640', 'CANADIAN COMMERCIAL CORPORATION-CCC'),
        ]
        base = {k: None for k in build.FIELDS['suppliers']}
        data = [dict(base, ocid=o, release_date=r, supplier_id=i, supplier_name=n, award_id='1') for o, r, i, n in rows]
        df = spark.createDataFrame(
            data, build.StructType([f for f in build.schema('suppliers').fields if f.name in base])
        )
        out = {(r.ocid): r for r in build.normalize_suppliers(df).collect()}
        # El nombre canónico es el de la publicación más reciente; el original se conserva.
        assert out['a'].supplier_name == 'ARRIETA NIÑO LUIS' and out['a'].supplier_name_original == 'ARRIETA NINO LUIS'
        assert out['a'].supplier_type == 'persona_natural' and out['c'].supplier_type == 'persona_juridica'
        assert out['d'].supplier_type == 'consorcio' and out['e'].supplier_type == 'consorcio'
        assert out['f'].supplier_type == 'extranjero'
    finally:
        spark.stop()
