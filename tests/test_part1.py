import pytest


@pytest.fixture(scope='module')
def spark():
    from src.config import spark_session

    session = spark_session()
    yield session
    session.stop()


@pytest.mark.integration
def test_hhi_matches_hand_calculation(spark):
    from src.part1.concentration import concentration

    # Grupo A: proveedores con 60, 30 y 10 -> shares 0.6, 0.3, 0.1 -> HHI = 10000*(0.36+0.09+0.01) = 4600.
    rows = [
        ('A', 's1', 60.0),
        ('A', 's2', 30.0),
        ('A', 's3', 10.0),
        ('B', 's1', 5.0),
        ('B', 's1', 5.0),
        ('B', 's9', None),
    ]
    df = spark.createDataFrame(rows, 'g string, supplier string, v double')
    metrics = {r.g: r for r in concentration(df, ['g'], 'supplier', 'v', top=2)[0].collect()}
    assert metrics['A'].hhi == pytest.approx(4600) and metrics['A'].top2_share == pytest.approx(0.9)
    assert metrics['A'].entities == 3 and metrics['A'].top1_share == pytest.approx(0.6)
    # B: un solo proveedor con monto (el nulo se ignora) -> monopolio.
    assert metrics['B'].hhi == pytest.approx(10000) and metrics['B'].entities == 1


@pytest.mark.integration
def test_rdd_and_dataframe_operations_are_equivalent(spark):
    from src.part1 import aggregations_dataframe as dataframe_ops
    from src.part1 import aggregations_rdd as rdd_ops
    from src.part1.benchmark import OPERATIONS, assert_equivalent, normalize

    processes = spark.createDataFrame(
        [
            ('o1', 'goods', 'LIMA', 'b1', 100.0),
            ('o2', 'goods', 'LIMA', 'b1', None),
            ('o3', 'works', 'CUSCO', 'b2', 50.0),
        ],
        'ocid string, category string, department string, buyer_id string, tender_amount_pen double',
    )
    suppliers = spark.createDataFrame(
        [('o1', 's1', 70.0), ('o1', 's2', 30.0), ('o2', 's1', 50.0), ('o3', 's3', 50.0), ('o3', 's4', None)],
        'ocid string, supplier_id string, allocated_amount_pen double',
    )
    for operation in OPERATIONS:
        reference = normalize(getattr(dataframe_ops, operation)(processes, suppliers).collect())
        assert_equivalent(reference, normalize(getattr(rdd_ops, operation)(processes, suppliers).collect()), operation)
    hhi = normalize(dataframe_ops.department_supplier_hhi(processes, suppliers).collect())
    # LIMA: s1 = 120, s2 = 30 -> (120² + 30²) / 150² = 0.68 -> 6800.
    assert hhi['LIMA'] == pytest.approx(6800) and hhi['CUSCO'] == pytest.approx(10000)


@pytest.mark.integration
def test_single_bidder_rate_ignores_missing(spark):
    from src.part1.eda import single_bidder_rates

    rows = [('LP', 'uno', 10.0), ('LP', 'dos a tres', 5.0), ('LP', 'sin dato', 1.0), ('AS', 'uno', 3.0)]
    df = spark.createDataFrame(rows, 'modality string, competition string, tender_amount_pen double')
    for column in ['category', 'department', 'amount_band', 'buyer_level']:
        df = df.withColumn(column, df.modality)
    df = df.withColumn('year', df.modality.substr(0, 0).cast('int'))
    rates = {r.value: r for r in single_bidder_rates(df).filter("dimension = 'modality' AND year = 'Todos'").collect()}
    assert rates['LP'].single_bidder_rate == pytest.approx(0.5) and rates['LP'].missing_share == pytest.approx(1 / 3)
    assert rates['AS'].single_bidder_rate == 1.0


@pytest.mark.integration
def test_exchange_count_ignores_cached_subtree(spark):
    from src.part1.partitioning import exchanges

    df = spark.createDataFrame([(str(i % 7), i) for i in range(100)], 'buyer_id string, v int')
    assert exchanges(df.groupBy('buyer_id').count()) == 1
    cached = df.repartition('buyer_id').cache()
    cached.count()
    # El shuffle del repartition ya ocurrió al llenar la caché; la agregación lo reutiliza.
    assert exchanges(cached.groupBy('buyer_id').count()) == 0
    cached.unpersist()


@pytest.mark.integration
def test_scan_filters_distinguish_partition_pruning(spark, tmp_path):
    from pyspark.sql import functions as F

    from src.part1.partitioning import scan_filters

    rows = [(y, f'{y}-0{m}-15', m) for y in (2023, 2024) for m in (1, 2)]
    spark.createDataFrame(rows, 'year int, effective_date string, month int').write.partitionBy('year').parquet(
        str(tmp_path / 't')
    )
    table = spark.read.parquet(str(tmp_path / 't'))
    by_partition = scan_filters(table.filter(F.col('year') == 2024))
    by_date = scan_filters(table.filter(F.col('effective_date').between('2024-01-01', '2024-12-31')))
    assert '2024' in by_partition['partition_filters'] and by_date['partition_filters'] == ''
    assert 'effective_date' in by_date['pushed_filters']


def test_participation_counts_wins_and_never_won(spark):
    from src.part1 import participation

    tenderers = spark.createDataFrame(
        [(f'p{i}', 'b1', 'A', 'Empresa A', 'persona_juridica') for i in range(3)]
        + [(f'p{i}', 'b1', 'B', 'Empresa B', 'persona_juridica') for i in range(25)]
        + [('p0', 'b1', 'N', 'Persona', 'persona_natural')],
        'ocid string, buyer_id string, tenderer_id string, tenderer_name string, tenderer_type string',
    )
    suppliers = spark.createDataFrame(
        [('p0', 'A', 100.0), ('p1', 'A', 50.0), ('p2', 'N', 10.0)],
        'ocid string, supplier_id string, allocated_amount_pen double',
    )
    stats = {r['id']: r for r in participation.participation(tenderers, suppliers).collect()}
    assert stats['A']['participations'] == 3 and stats['A']['wins'] == 2
    assert stats['A']['win_rate'] == pytest.approx(2 / 3) and stats['A']['amount_won_pen'] == 150
    assert stats['B']['wins'] == 0 and stats['B']['participations'] == 25
    rankings = participation.participation_rankings(participation.participation(tenderers, suppliers)).collect()
    never = [r['id'] for r in rankings if r['ranking'] == 'nunca_ganan']
    assert never == ['B']  # 25 participaciones sin ganar (mínimo 20)
    assert all(r['id'] != 'N' for r in rankings)  # personas naturales sin nombre en los rankings


@pytest.mark.integration
def test_adjusted_competition_uses_the_competitive_mix(spark):
    from src.part1.eda import adjusted_competition

    # Competitivas: 30 procesos de bienes (3 con un postor) y 10 de servicios (5): pesos 0.75 y 0.25.
    # Directa: bienes 10 de 20 con un postor (0.5) y servicios 20 de 20 (1.0).
    def block(modality, category, total, single):
        return [
            (modality, category, 'LIMA', 2024, 'bajo', 'uno' if i < single else 'cuatro o mas') for i in range(total)
        ]

    rows = (
        block('Licitación Pública', 'goods', 30, 3)
        + block('Licitación Pública', 'services', 10, 5)
        + block('Contratación Directa', 'goods', 20, 10)
        + block('Contratación Directa', 'services', 20, 20)
        + [('Contratación Directa', 'goods', 'LIMA', 2024, 'bajo', 'sin dato')]
    )
    df = spark.createDataFrame(
        rows, 'modality string, category string, department string, year int, amount_band string, competition string'
    )
    result, persistence = adjusted_competition(df)
    direct = next(r for r in result if r['level'] == 'modalidad' and r['name'] == 'Contratación Directa')
    assert direct['informed'] == 40 and direct['raw_rate'] == pytest.approx(0.75)
    # Con la mezcla de las competitivas: 0.75 * 0.5 + 0.25 * 1.0 = 0.625.
    assert direct['adjusted_rate'] == pytest.approx(0.625) and direct['strata_coverage'] == pytest.approx(1)
    # Servicios no llega a 20 procesos competitivos: solo se compara el estrato de bienes.
    assert persistence['comparable_strata'] == 1 and persistence['strata_where_direct_is_higher'] == 1
