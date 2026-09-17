import json
import time
import numpy as np
import pandas as pd
import hnswlib
from threadpoolctl import threadpool_limits
from sklearn.feature_extraction.text import TfidfVectorizer
from src.config import ANN_SAMPLE, SEED, ARTIFACTS, spark_session
from src.common import sample_processes, save_csv, save_json


@threadpool_limits.wrap(limits=1)
def run(spark):
    data=sample_processes(spark,ANN_SAMPLE)
    data=data[data.normalized_text.str.len()>0].reset_index(drop=True)
    if len(data)<3: raise ValueError('ANN requiere tres textos')
    vectorizer=TfidfVectorizer(ngram_range=(1,2),max_features=4096,dtype=np.float32,norm='l2')
    x=vectorizer.fit_transform(data.normalized_text).toarray()
    keep=np.linalg.norm(x,axis=1)>0; x=x[keep]; data=data[keep].reset_index(drop=True)
    queries=np.random.default_rng(SEED).choice(len(x),size=min(40,len(x)),replace=False)
    kmax=min(10,len(x)-1)
    exact={}; times=[]; display=[]; exact_rows=[]
    for q in queries:
        start=time.perf_counter(); sims=x@x[q]; sims[q]=-np.inf
        ids=np.lexsort((np.arange(len(x)),-sims))[:kmax]; times.append(time.perf_counter()-start); exact[int(q)]=ids
        for rank,j in enumerate(ids,1):
            r=dict(config='exact',query_ocid=data.iloc[q].ocid,neighbor_ocid=data.iloc[j].ocid,rank=rank,similarity=float(sims[j]))
            exact_rows.append(r); display.append(r)
    grid=[]
    out=ARTIFACTS/'ann'; out.mkdir(parents=True,exist_ok=True)
    for m in [8,16]:
        for construction in [100,200]:
            print(f'ANN: M={m}, efConstruction={construction}, corpus={len(x)}',flush=True)
            start=time.perf_counter(); index=hnswlib.Index(space='cosine',dim=x.shape[1])
            index.init_index(max_elements=len(x),ef_construction=construction,M=m,random_seed=SEED)
            index.set_num_threads(1); index.add_items(x,np.arange(len(x)),num_threads=1)
            build=time.perf_counter()-start
            path=out/f'hnsw_M{m}_efC{construction}.bin'; index.save_index(str(path))
            for ef in [20,50,100]:
                index.set_ef(max(ef,kmax+1)); latencies=[]; recalls={5:[],10:[]}
                config=f'M{m}_efC{construction}_efS{ef}'
                for q in queries:
                    start=time.perf_counter(); labels,dist=index.knn_query(x[q],k=kmax+1,num_threads=1); latencies.append(time.perf_counter()-start)
                    neighbors=[(int(j),float(1-d)) for j,d in zip(labels[0],dist[0]) if j!=q][:kmax]
                    for k in [5,10]:
                        kk=min(k,kmax); recalls[k].append(len(set(j for j,_ in neighbors[:kk])&set(exact[int(q)][:kk]))/kk)
                    for rank,(j,sim) in enumerate(neighbors,1):
                        display.append(dict(config=config,query_ocid=data.iloc[q].ocid,neighbor_ocid=data.iloc[j].ocid,rank=rank,similarity=sim))
                grid.append(dict(config=config,M=m,efConstruction=construction,efSearch=ef,recall_at_5=float(np.mean(recalls[5])),recall_at_10=float(np.mean(recalls[10])),mean_ms=1000*float(np.mean(latencies)),median_ms=1000*float(np.median(latencies)),p95_ms=1000*float(np.percentile(latencies,95)),exact_mean_ms=1000*float(np.mean(times)),speedup=float(np.mean(times)/np.mean(latencies)),build_seconds=build,index_bytes=path.stat().st_size,corpus_size=len(x),queries=len(queries),kmax=kmax))
    save_csv('ann/hnsw_grid.csv',grid)
    save_csv('ann/neighbors_display.csv',display)
    pd.DataFrame(exact_rows).to_parquet(out/'exact_neighbors.parquet',index=False)
    save_csv('ann/documents.csv',data[['ocid','text','year','category']])
    save_json('ann/queries.json',[dict(ocid=data.iloc[q].ocid,text=data.iloc[q].text) for q in queries])
    save_json('ann/vectorizer.json',{'vocabulary':{k:int(v) for k,v in vectorizer.vocabulary_.items()},'idf':vectorizer.idf_.tolist(),'ngram_range':[1,2],'normalization':'l2','max_features':4096,'seed':SEED,'corpus_size':len(x),'note':'Mismo TF-IDF para exacto y HNSW, sin reducción dimensional; empates de distancia pueden reducir recall de identificadores.'})
    save_json('ann/ivf_status.json',{'implemented':False,'reason':'Bono opcional; comparación obligatoria exacto frente a HNSW implementada.'})

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
