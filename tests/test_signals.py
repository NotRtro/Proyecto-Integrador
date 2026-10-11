import pytest


@pytest.fixture(scope='module')
def spark():
    from src.config import spark_session

    session = spark_session()
    yield session
    session.stop()


@pytest.mark.integration
def test_signal_rates_and_cooccurrence(spark):
    from src.signals.run import SIGNALS, cooccurrence, rates

    # Cuatro procesos; el nulo significa que la señal no se puede evaluar en ese proceso.
    rows = [
        ('A', 1, 0, 1, 1),
        ('A', 0, 0, None, 1),
        ('A', None, 1, 0, 0),
        ('B', 0, 0, 0, 0),
    ]
    df = spark.createDataFrame(
        rows, 'g string, single_bidder int, direct int, dominant_supplier int, repeated_purchase int'
    )
    by_group = {(r['g'], r['signal']): r for r in rates(df, ['g'])}
    single = by_group['A', 'single_bidder']
    assert (single['processes'], single['evaluable'], single['flagged']) == (3, 2, 1)
    assert single['rate'] == pytest.approx(0.5) and single['not_evaluable_share'] == pytest.approx(1 / 3)
    assert by_group['B', 'direct']['rate'] == 0
    assert {r['signal'] for r in by_group.values()} == set(SIGNALS)

    pairs = {(r['signal_a'], r['signal_b']): r for r in cooccurrence(df)}
    # Evaluables en ambas: 3 procesos; 1 y 2 marcados -> esperado 2/3; coinciden 1 -> lift 1.5.
    both = pairs['single_bidder', 'repeated_purchase']
    assert both['evaluable_both'] == 3 and both['joint'] == 1 and both['lift'] == pytest.approx(1.5)
    # Sin ningún caso de una de las dos señales, el lift no está definido.
    assert pairs['single_bidder', 'direct']['lift'] is None
