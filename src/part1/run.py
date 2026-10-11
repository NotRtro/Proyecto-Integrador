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
    bidders_by_modality.csv                    qué cuenta el número de postores
    single_bidder_adjusted.csv                 tasa por modalidad a igual mezcla de estratos
    item_segments.csv, item_codes.csv, item_segments_by_year.csv, item_summary.json   frecuencia de ítems
    participation_rankings.csv, participation_bins.csv, participation_summary.json
                                               quién participa, quién gana y quién nunca gana (Q1, Q2)
    rdd_dataframe_benchmark.csv, execution_plans.json, code_snippets.json
    partitioning_experiment.json, spark_vs_pandas.csv
"""

from pyspark.sql import functions as F

from src.common import StepTimer, experiment, save_csv, save_json
from src.common import processes as read_processes
from src.config import PROCESSED, YEARS, spark_session
from src.ingestion.validate_files import input_files
from src.part1 import concentration, eda, participation
from src.part1.benchmark import run_benchmark
from src.part1.partitioning import run_experiment
from src.part1.sampling import sampling_effect
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
        export('bidders_by_modality', eda.bidders_by_modality(processes))
        adjusted, persistence = eda.adjusted_competition(processes)
        save_csv('eda/single_bidder_adjusted.csv', adjusted)
        save_json('eda/single_bidder_persistence.json', persistence)
        # La categoría viene del proceso; se agrega aquí porque supplier_frame solo trae contexto geográfico.
        suppliers_with_category = suppliers.join(processes.select('ocid', 'category'), 'ocid', 'left')
        supplier_cube = suppliers_with_category.groupBy(
            'year',
            'month',
            'department',
            'category',
            'buyer_id',
            'buyer_name',
            'supplier_id',
            'supplier_name',
            'supplier_type',
        ).agg(F.sum('allocated_amount_pen').alias('allocated_amount_pen'), F.countDistinct('ocid').alias('processes'))
        export('supplier_cube', supplier_cube)

    with timer.step('concentración (Q1)'):
        market = concentration.market_concentration(processes, suppliers).toPandas()
        save_json('eda/concentration_metrics.json', market.to_dict(orient='records'))
        buyer_hhi = concentration.buyer_supplier_concentration(suppliers).cache()
        export('buyer_supplier_hhi', buyer_hhi)
        export('buyer_hhi_summary', concentration.buyer_hhi_summary(buyer_hhi))
        export('department_supplier_hhi', concentration.department_supplier_concentration(suppliers))
        export(
            'top_buyers',
            concentration.top_rankings(
                processes, ['year'], 'buyer_id', ['buyer_name', 'buyer_level'], 'award_amount_pen'
            ),
        )
        export(
            'top_suppliers',
            concentration.top_rankings(
                suppliers, ['year'], 'supplier_id', ['supplier_name', 'supplier_type'], 'allocated_amount_pen'
            ),
        )
        buyer_hhi.unpersist()

    with timer.step('frecuencia de ítems y participación'):
        items = spark.read.parquet(str(PROCESSED / 'items'))
        by_segment, by_code, by_year, item_summary = participation.item_frequency(spark, items)
        export('item_segments', by_segment)
        export('item_codes', by_code)
        export('item_segments_by_year', by_year)
        save_json('eda/item_summary.json', item_summary)
        tenderers = spark.read.parquet(str(PROCESSED / 'tenderers'))
        stats = participation.participation(tenderers, raw_suppliers).cache()
        export('participation_rankings', participation.participation_rankings(stats))
        export('participation_bins', participation.participation_bins(stats))
        save_json('eda/participation_summary.json', participation.participation_summary(stats, tenderers))
        stats.unpersist()

    # Mediciones de una vez (ver src/common.py: experiment): se repiten o se reutilizan según RUN_EXPERIMENTS.
    with timer.step('RDD frente a DataFrame'):

        def benchmark():
            # Las operaciones del benchmark hacen su propio join con el departamento: tabla original.
            results, plans, snippets = run_benchmark(processes, raw_suppliers)
            save_csv('eda/rdd_dataframe_benchmark.csv', results)
            save_json('eda/execution_plans.json', plans)
            save_json('eda/code_snippets.json', snippets)

        experiment('rdd_vs_dataframe', ['eda/rdd_dataframe_benchmark.csv', 'eda/code_snippets.json'], benchmark)
    with timer.step('efecto de muestrear la Q1'):
        experiment(
            'muestreo_q1',
            ['eda/sampling_effect.csv'],
            lambda: save_csv('eda/sampling_effect.csv', sampling_effect(suppliers)),
        )

    processes.unpersist()
    suppliers.unpersist()
    with timer.step('experimento de particionamiento'):
        experiment(
            'particionamiento',
            ['eda/partitioning_experiment.json'],
            lambda: save_json(
                'eda/partitioning_experiment.json',
                run_experiment(spark, [str(p) for p in input_files()], YEARS[len(YEARS) // 2]),
            ),
        )
    with timer.step('Spark frente a pandas'):
        experiment(
            'spark_vs_pandas',
            ['eda/spark_vs_pandas.csv'],
            lambda: save_csv('eda/spark_vs_pandas.csv', run_comparison(spark)),
        )
    save_json('eda/part1_timings.json', timer.steps)


if __name__ == '__main__':
    session = spark_session()
    try:
        run(session)
    finally:
        session.stop()
