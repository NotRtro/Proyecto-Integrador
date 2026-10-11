"""Validación final de los artefactos antes de servirlos en la web.

Motor: local (pandas sobre archivos pequeños). Comprueba que existan los artefactos que
usa la web y que se cumplan invariantes que cualquier error de cálculo rompería:
conservación de montos, rangos de métricas, garantías teóricas de las estructuras
aproximadas y equivalencias entre algoritmos. Si un control falla, la corrida falla.

Artefacto: metadata/artifact_validation.json (controles y pendientes académicos).
"""
import json
import math

import pandas as pd

from src.common import now, save_json
from src.config import ARTIFACTS, PROCESSED

REQUIRED = [
    'metadata/source_manifest.json', 'metadata/quality_summary.json', 'metadata/exchange_rates.csv',
    'metadata/domain_stopwords.json', 'metadata/processing_timings.json',
    'eda/filter_cube.csv', 'eda/supplier_cube.csv', 'eda/amount_outlier_summary.csv', 'eda/amount_histogram.csv',
    'eda/rdd_dataframe_benchmark.csv', 'eda/code_snippets.json', 'eda/partitioning_experiment.json',
    'eda/spark_vs_pandas.csv', 'eda/concentration_metrics.json', 'eda/buyer_supplier_hhi.csv',
    'eda/single_bidder_rates.csv',
    'similarity/lsh_parameter_grid.csv', 'similarity/minhash_error.csv', 'similarity/s_curve_observed.csv',
    'similarity/methodology.json', 'similarity/repeated_summary.csv',
    'ann/hnsw_grid.csv', 'ann/ivf_grid.csv', 'ann/scaling.csv', 'ann/methodology.json',
    'streaming/bloom_grid.csv', 'streaming/cms_grid.csv', 'streaming/dgim_grid.csv', 'streaming/live_params.json',
    'association/rules.csv', 'association/selected_rules.json', 'association/single_bidder_rules.csv',
]


def read(name):
    path = ARTIFACTS / name
    return json.loads(path.read_text(encoding='utf-8')) if path.suffix == '.json' else pd.read_csv(path)


def between(series, low=0.0, high=1.0):
    return bool(series.dropna().between(low, high).all())


def monetary_checks():
    processes = pd.read_parquet(PROCESSED / 'processes', columns=['ocid', 'tender_amount_pen', 'award_amount_pen'])
    cube = read('eda/filter_cube.csv')
    awards = pd.read_parquet(PROCESSED / 'awards', columns=['amount_pen', 'supplier_count'])
    suppliers = pd.read_parquet(PROCESSED / 'suppliers', columns=['allocated_amount_pen'])
    checks = {
        'unique_ocid': not processes.ocid.duplicated().any(),
        'eda_process_count': int(cube.processes.sum()) == len(processes),
        'supplier_allocation_conserves_amount': math.isclose(
            float(awards.loc[awards.supplier_count > 0, 'amount_pen'].sum()),
            float(suppliers.allocated_amount_pen.sum()), rel_tol=1e-10, abs_tol=.01),
    }
    for column in ['tender_amount_pen', 'award_amount_pen']:
        checks[f'eda_conservation_{column}'] = math.isclose(float(cube[column].sum()), float(processes[column].sum()),
                                                            rel_tol=1e-10, abs_tol=.01)
    return checks


def analytic_checks():
    minhash = read('similarity/minhash_error.csv').sort_values('hashes')
    corpus = read('similarity/methodology.json')['full_corpus']
    scaling = read('ann/scaling.csv').sort_values('corpus_size')
    cms = read('streaming/cms_grid.csv')
    dgim = read('streaming/dgim_grid.csv')
    bench = read('association/algorithm_benchmark.csv')
    rules = read('association/rules.csv')
    return {
        'hhi_in_range': all(0 <= r['hhi'] <= 10000 for r in read('eda/concentration_metrics.json'))
                        and between(read('eda/buyer_supplier_hhi.csv').hhi, 0, 10000),
        'single_bidder_rate_in_range': between(read('eda/single_bidder_rates.csv').single_bidder_rate),
        'minhash_error_decreases_with_k': bool(minhash.rmse_similar.iloc[0] > minhash.rmse_similar.iloc[-1]),
        'lsh_metrics_in_range': all(between(read('similarity/lsh_parameter_grid.csv')[c]) for c in ['precision', 'recall', 'f1']),
        'lsh_compares_fewer_pairs': 0 < corpus['similar_pairs'] <= corpus['candidate_pairs'] < corpus['possible_pairs'],
        'ann_recall_in_range': all(between(read(f)[c]) for f in ['ann/hnsw_grid.csv', 'ann/ivf_grid.csv']
                                   for c in ['recall_at_5', 'recall_at_10']),
        'exact_search_grows_with_corpus': bool(scaling.exact_mean_ms.iloc[-1] > scaling.exact_mean_ms.iloc[0]),
        'bloom_zero_false_negatives': bool((read('streaming/bloom_grid.csv').fn == 0).all()),
        'cms_no_underestimate': bool((cms.underestimates == 0).all()),
        'cms_within_error_bound': bool((cms.within_bound >= cms.guaranteed).all()),
        'dgim_within_bound': bool((dgim.relative_error.dropna() <= 0.5).all()),
        'apriori_fpgrowth_equal': bool(bench.loc[bench.scope == 'same_sample', 'equivalent'].all()),
        'rules_metrics_in_range': between(rules.support) and between(rules.confidence) and bool((rules.lift >= 0).all()),
    }


def run():
    missing = [p for p in REQUIRED if not (ARTIFACTS / p).is_file()]
    if missing:
        raise RuntimeError(f'Faltan artefactos: {missing}')
    checks = {**monetary_checks(), **analytic_checks()}
    selected = read('association/selected_rules.json')
    pending = ['Interpretaciones del equipo en frontend/src/content (hallazgos, reglas y casos de similitud)',
               'Validación humana en SEACE de los casos de similitud', 'Grabación o publicación de la demostración']
    if selected['found'] < 8:
        pending.append('Menos de ocho reglas superan los criterios')
    result = {'checked_at': now(), 'checks': checks, 'selected_rules': selected['found'], 'academic_pending': pending,
              'files': [{'path': p, 'bytes': (ARTIFACTS / p).stat().st_size} for p in REQUIRED]}
    save_json('metadata/artifact_validation.json', result)
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f'Validación final falló: {failed}')
    return result


if __name__ == '__main__':
    run()
