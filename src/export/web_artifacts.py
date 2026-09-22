import json
import math
import pandas as pd
from src.config import ARTIFACTS, PROCESSED
from src.common import save_json,now
REQUIRED=['metadata/source_manifest.json','metadata/quality_summary.json','metadata/exchange_rates.csv','eda/filter_cube.csv','eda/amount_outlier_summary.csv','eda/rdd_dataframe_benchmark.csv','similarity/lsh_parameter_grid.csv','ann/hnsw_grid.csv','streaming/bloom_grid.csv','streaming/cms_grid.csv','streaming/dgim_grid.csv','association/rules.csv','association/selected_rules.json']

def run():
    missing=[p for p in REQUIRED if not (ARTIFACTS/p).is_file()]
    if missing: raise RuntimeError(f'Faltan artefactos: {missing}')
    checks={}
    p=pd.read_parquet(PROCESSED/'processes',columns=['ocid','tender_amount_pen','award_amount_pen'])
    cube=pd.read_csv(ARTIFACTS/'eda/filter_cube.csv')
    checks['unique_ocid']=not p.ocid.duplicated().any()
    checks['eda_process_count']=int(cube.processes.sum())==len(p)
    for col in ['tender_amount_pen','award_amount_pen']:
        checks[f'eda_conservation_{col}']=math.isclose(float(cube[col].sum()),float(p[col].sum()),rel_tol=1e-10,abs_tol=.01)
    awards=pd.read_parquet(PROCESSED/'awards',columns=['amount_pen','supplier_count'])
    suppliers=pd.read_parquet(PROCESSED/'suppliers',columns=['allocated_amount_pen'])
    checks['supplier_allocation_conserves_amount']=math.isclose(float(awards.loc[awards.supplier_count>0,'amount_pen'].sum()),float(suppliers.allocated_amount_pen.sum()),rel_tol=1e-10,abs_tol=.01)
    bloom=pd.read_csv(ARTIFACTS/'streaming/bloom_grid.csv'); cms=pd.read_csv(ARTIFACTS/'streaming/cms_grid.csv')
    checks['bloom_zero_false_negatives']=bool((bloom.fn==0).all())
    checks['cms_no_underestimate']=bool((cms.estimate>=cms.exact).all())
    for file,cols in [('similarity/lsh_parameter_grid.csv',['precision','recall','f1']),('ann/hnsw_grid.csv',['recall_at_5','recall_at_10'])]:
        df=pd.read_csv(ARTIFACTS/file)
        checks[file]=all(bool(df[col].between(0,1).all()) for col in cols)
    bench=pd.read_csv(ARTIFACTS/'association/algorithm_benchmark.csv')
    checks['apriori_fpgrowth_equal']=bool(bench.loc[bench.scope=='same_sample','equivalent'].all())
    selected=json.loads((ARTIFACTS/'association/selected_rules.json').read_text())
    result={'checked_at':now(),'checks':checks,'files':[{'path':p,'bytes':(ARTIFACTS/p).stat().st_size} for p in REQUIRED], 'selected_rules':selected['found'],'academic_pending':['Validación humana de casos LSH interpretados con asistencia de IA','Grabación o publicación de demostración']}
    if selected['found']<8: result['academic_pending'].append('Menos de ocho reglas superan los criterios')
    save_json('metadata/artifact_validation.json',result)
    failed=[name for name,ok in checks.items() if not ok]
    if failed: raise ValueError(f'Validación final falló: {failed}')
    return result

if __name__=='__main__': run()
