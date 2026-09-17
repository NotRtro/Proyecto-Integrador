import itertools
import json
from pathlib import Path
import time
from src.config import SAMPLE, SEED, spark_session
from src.common import sample_processes, save_csv, save_json
from src.part2.exact_jaccard import jaccard
from src.part2.minhash_lsh import signature, band_keys


def run(spark):
    data=sample_processes(spark,SAMPLE)
    data=data[data.shingles.map(len)>0].reset_index(drop=True)
    if len(data)<2: raise ValueError('LSH requiere al menos dos textos')
    sets=[set(x) for x in data.shingles]
    start=time.perf_counter()
    exact={(i,j):jaccard(sets[i],sets[j]) for i in range(len(data)) for j in range(i+1,len(data))}
    exact_time=time.perf_counter()-start
    signatures=spark.sparkContext.parallelize(list(enumerate(data.shingles.tolist()))).map(lambda r:(r[0],signature(r[1],128,SEED))).cache()
    signatures.count()
    grid=[]; pairs=[]
    for hashes in [32,64,128]:
        for rows in [2,4,8]:
            bands=hashes//rows
            start=time.perf_counter()
            candidates=set(signatures.flatMap(lambda r:band_keys(r,bands,rows)).groupByKey().flatMap(lambda kv:itertools.combinations(sorted(set(kv[1])),2)).distinct().collect())
            elapsed=time.perf_counter()-start
            for threshold in [.5,.6,.7,.8]:
                truth={p for p,v in exact.items() if v>=threshold}
                tp=len(candidates&truth); fp=len(candidates-truth); fn=len(truth-candidates)
                precision=tp/len(candidates) if candidates else 0.; recall=tp/len(truth) if truth else 0.
                config=f'h{hashes}_b{bands}_r{rows}_t{threshold}'
                grid.append(dict(config=config,hashes=hashes,bands=bands,rows=rows,threshold=threshold,precision=precision,recall=recall,f1=2*precision*recall/(precision+recall) if precision+recall else 0,tp=tp,fp=fp,fn=fn,candidates=len(candidates),exact_seconds=exact_time,lsh_seconds=elapsed,sample_size=len(data),truth_pairs=len(truth)))
                # Every evaluated positive/candidate is retained, with exact verification.
                for i,j in sorted(candidates|truth):
                    status='TP' if (i,j) in candidates&truth else 'FP' if (i,j) in candidates else 'FN'
                    pairs.append(dict(config=config,left_ocid=data.iloc[i].ocid,right_ocid=data.iloc[j].ocid,jaccard=exact[i,j],status=status,left_year=int(data.iloc[i].year),right_year=int(data.iloc[j].year),left_category=data.iloc[i].category,right_category=data.iloc[j].category))
    signatures.unpersist()
    save_csv('similarity/lsh_parameter_grid.csv',grid); save_csv('similarity/precision_recall_curve.csv',grid)
    columns=['config','left_ocid','right_ocid','jaccard','status','left_year','right_year','left_category','right_category']
    save_csv('similarity/similar_pairs.csv',pairs,columns)
    save_csv('similarity/false_positives.csv',[p for p in pairs if p['status']=='FP'],columns)
    save_csv('similarity/false_negatives.csv',[p for p in pairs if p['status']=='FN'],columns)
    save_csv('similarity/documents.csv',data[['ocid','year','category','text']])
    notes=json.loads(Path(__file__).with_name('case_notes.json').read_text())
    ids=set(data.ocid)
    notes['cases']=[c for c in notes['cases'] if c['left_ocid'] in ids and c['right_ocid'] in ids]
    notes['suggested_cases']=sorted(pairs,key=lambda x:-x['jaccard'])[:3]
    save_json('similarity/reviewed_cases.json',notes)
    save_json('similarity/methodology.json',{'sample':'Menores SHA256(ocid + semilla), reproducible', 'seed':SEED,'size':len(data),'representation':'Shingles de dos palabras; fallback tokens para textos cortos','metric':'Precisión y recall de candidatos antes de verificación exacta; umbrales con cero pares verdaderos no son evidencia de recuperación','exact_seconds':exact_time})

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
