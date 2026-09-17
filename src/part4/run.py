from collections import Counter,deque
import sys
import time
import math
from src.config import spark_session
from src.common import processes,save_csv,save_json
from src.part4.bloom_filter import BloomFilter
from src.part4.count_min_sketch import CountMinSketch
from src.part4.dgim import DGIM


def run(spark):
    events=processes(spark).select('effective_date','ocid','buyer_id','category','tender_amount_pen').orderBy('effective_date','ocid').toLocalIterator()
    blooms={(bits,k):BloomFilter(bits,k) for bits in [8192,65536,524288] for k in [3,5,7]}
    cms={(w,d):CountMinSketch(w,d) for w in [128,512,2048] for d in [3,5]}
    dgims={(w,t):DGIM(w) for w in [100,1000,10000] for t in [100000,1000000,10000000]}
    seen=set(); counts=Counter(); bloom_stats={key:Counter() for key in blooms}; bloom_time=Counter(); cms_time=Counter(); dgim_time=Counter()
    windows={key:deque() for key in dgims}; sums=Counter(); series=[]; memory=[]; n=0
    for n,event in enumerate(events,1):
        if n%25000==0: print(f'Flujos: {n:,} eventos',flush=True)
        entity=event.buyer_id
        if entity:
            existed=entity in seen
            for key,b in blooms.items():
                start=time.perf_counter(); predicted=entity in b; b.add(entity); bloom_time[key]+=time.perf_counter()-start
                bloom_stats[key]['fp']+=predicted and not existed; bloom_stats[key]['fn']+=not predicted and existed; bloom_stats[key]['negative_queries']+=not existed; bloom_stats[key]['queries']+=1
            seen.add(entity)
        category=event.category or 'sin dato'; counts[category]+=1
        for key,c in cms.items():
            start=time.perf_counter(); c.add(category); cms_time[key]+=time.perf_counter()-start
        for key,d in dgims.items():
            window,threshold=key; bit=int(event.tender_amount_pen is not None and event.tender_amount_pen>=threshold)
            start=time.perf_counter(); d.add(bit); dgim_time[key]+=time.perf_counter()-start
            windows[key].append(bit); sums[key]+=bit
            if len(windows[key])>window: sums[key]-=windows[key].popleft()
        if n%max(100,500)==0:
            for (w,t),d in dgims.items():
                exact=sums[w,t]; estimate=d.query()
                series.append(dict(event=n,date=event.effective_date,window=w,threshold=t,exact=exact,estimate=estimate,absolute_error=abs(exact-estimate),relative_error=abs(exact-estimate)/exact if exact else None,buckets=len(d.buckets)))
    bloom_rows=[]
    for (bits,k),b in blooms.items():
        stats=bloom_stats[bits,k]; exact_bytes=sys.getsizeof(seen)+sum(sys.getsizeof(v) for v in seen)
        bloom_rows.append(dict(bits=bits,hashes=k,inserted=len(seen),**stats,observed_fpr=stats['fp']/max(1,stats['negative_queries']),theoretical_fpr=b.theoretical_fpr(len(seen)),bytes=len(b.data),exact_bytes=exact_bytes,operation_us=1e6*bloom_time[bits,k]/max(1,stats['queries'])))
        memory.append(dict(algorithm='Bloom',config=f'{bits}/{k}',approx_bytes=len(b.data),exact_bytes=exact_bytes))
    cms_rows=[]
    for (w,d),c in cms.items():
        for value,exact in counts.items():
            start=time.perf_counter(); estimate=c.query(value); elapsed=time.perf_counter()-start
            cms_rows.append(dict(width=w,depth=d,value=value,exact=exact,estimate=estimate,absolute_error=estimate-exact,relative_error=(estimate-exact)/exact,bytes=c.bytes,update_us=1e6*cms_time[w,d]/max(1,n),query_us=1e6*elapsed,epsilon=math.e/w,delta=math.exp(-d)))
        memory.append(dict(algorithm='CMS',config=f'{w}/{d}',approx_bytes=c.bytes,exact_bytes=sys.getsizeof(counts)+sum(sys.getsizeof(k)+sys.getsizeof(v) for k,v in counts.items())))
    dgim_rows=[]
    for (w,t),d in dgims.items():
        exact=sums[w,t]; estimate=d.query(); size=sys.getsizeof(d.buckets)+sum(sys.getsizeof(b)+sum(sys.getsizeof(v) for v in b) for b in d.buckets)
        dgim_rows.append(dict(window=w,threshold=t,exact=exact,estimate=estimate,absolute_error=abs(exact-estimate),relative_error=abs(exact-estimate)/exact if exact else None,buckets=len(d.buckets),bytes=size,operation_us=1e6*dgim_time[w,t]/max(1,n)))
        memory.append(dict(algorithm='DGIM',config=f'{w}/{t}',approx_bytes=size,exact_bytes=sys.getsizeof(windows[w,t])))
    save_csv('streaming/bloom_grid.csv',bloom_rows); save_csv('streaming/cms_grid.csv',cms_rows); save_csv('streaming/dgim_grid.csv',dgim_rows)
    save_csv('streaming/error_over_time.csv',series,['event','date','window','threshold','exact','estimate','absolute_error','relative_error','buckets']); save_csv('streaming/memory_comparison.csv',memory)
    save_json('streaming/methodology.json',{'events':n,'ordering':'effective_date, ocid','bloom':'Consulta antes de insertar entidad. FPR observado sobre entidades vistas por primera vez; teórico corresponde a ocupación final.','cms':'Frecuencia de categoría; dominio pequeño puede dar error cero.','dgim':'Alto monto según umbrales fijos ex ante; no usa cuantiles futuros.','memory':'Bloom/CMS: buffers; referencias exactas y DGIM: estimaciones sys.getsizeof. No equivale a RSS del proceso.'})

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
