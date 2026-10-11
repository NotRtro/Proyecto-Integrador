"""Parte I: EDA, concentración, RDD frente a DataFrame y particionamiento.

Motor: Spark en todo el cálculo (exigido por el enunciado). Solo se traen al driver
resultados ya agregados para escribirlos como CSV/JSON para la web.

Artefactos (artifacts/eda/):
    filter_cube.csv, supplier_cube.csv         cubos para los filtros de la web
    *_distribution.csv, monthly_evolution.csv  distribuciones (tiempo, categoría, modalidad...)
    amount_histogram.csv, amount_percentiles.csv, amount_outlier_summary.csv
    sql_yearly_totals.csv                      totales anuales en Spark SQL
    concentration_metrics.json                 HHI nacional por año (entidades y proveedores)
    buyer_supplier_hhi.csv, buyer_hhi_summary.csv, department_supplier_hhi.csv   (Q1)
    top_buyers.csv, top_suppliers.csv          rankings con participación
    single_bidder_rates.csv                    tasa de postor único por dimensión (Q2)
    rdd_dataframe_benchmark.csv, execution_plans.json, code_snippets.json
    partitioning_experiment.json, spark_vs_pandas.csv
"""
from pyspark.sql import functions as F

from src.common import StepTimer, processes as read_processes, save_csv, save_json
from src.config import PROCESSED, YEARS, spark_session
from src.ingestion.validate_files import input_files
from src.part1 import concentration, eda
from src.part1.benchmark import run_benchmark
from src.part1.partitioning import run_experiment
from src.part1.spark_vs_pandas import run_comparison


def export(name, df):
    save_csv(f'eda/{name}.csv', df.toPandas())


def run(spark):
    timer = StepTimer()
    processes = eda.with_informed_categories(read_processes(spark)).cache()
    raw_suppliers = spark.read.parquet(str(PROCESSED / 'suppliers'))
    suppliers = concentration.supplier_frame(raw_suppliers, processes).cache()
    processes.count()
    suppliers.count()

    with timer.step('EDA y cubos'):
        export('filter_cube', eda.filter_cube(processes))
        for name, df in eda.distributions(processes).items():
            export(name, df)
        export('sql_yearly_totals', eda.yearly_totals_sql(spark, processes))
        export('amount_histogram', eda.amount_histogram(processes))
        export('amount_percentiles', eda.amount_percentiles(processes))
        export('amount_outlier_summary', eda.amount_outliers(processes))
        export('single_bidder_rates', eda.single_bidder_rates(processes))
        # La categoría viene del proceso; se agrega aquí porque supplier_frame solo trae contexto geográfico.
        suppliers_with_category = suppliers.join(processes.select('ocid', 'category'), 'ocid', 'left')
        supplier_cube = (suppliers_with_category
                         .groupBy('year', 'month', 'department', 'category', 'buyer_id', 'buyer_name',
                                  'supplier_id', 'supplier_name', 'supplier_type')
                         .agg(F.sum('allocated_amount_pen').alias('allocated_amount_pen'),
                              F.countDistinct('ocid').alias('processes')))
        export('supplier_cube', supplier_cube)

    with timer.step('concentración (Q1)'):
        market = concentration.market_concentration(processes, suppliers).toPandas()
        save_json('eda/concentration_metrics.json', market.to_dict(orient='records'))
        buyer_hhi = concentration.buyer_supplier_concentration(suppliers).cache()
        export('buyer_supplier_hhi', buyer_hhi)
        export('buyer_hhi_summary', concentration.buyer_hhi_summary(buyer_hhi))
        export('department_supplier_hhi', concentration.department_supplier_concentration(suppliers))
        export('top_buyers', concentration.top_rankings(processes, ['year'], 'buyer_id', ['buyer_name', 'buyer_level'],
                                                        'award_amount_pen'))
        export('top_suppliers', concentration.top_rankings(suppliers, ['year'], 'supplier_id',
                                                           ['supplier_name', 'supplier_type'], 'allocated_amount_pen'))
        buyer_hhi.unpersist()

    with timer.step('RDD frente a DataFrame'):
        # Las operaciones del benchmark hacen su propio join con el departamento: tabla original.
        results, plans, snippets = run_benchmark(processes, raw_suppliers)
        save_csv('eda/rdd_dataframe_benchmark.csv', results)
        save_json('eda/execution_plans.json', plans)
        save_json('eda/code_snippets.json', snippets)

    processes.unpersist()
    suppliers.unpersist()
    with timer.step('experimento de particionamiento'):
        save_json('eda/partitioning_experiment.json',
                  run_experiment(spark, [str(p) for p in input_files()], YEARS[len(YEARS) // 2]))
    with timer.step('Spark frente a pandas'):
        save_csv('eda/spark_vs_pandas.csv', run_comparison(spark))
    save_json('eda/part1_timings.json', timer.steps)


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
