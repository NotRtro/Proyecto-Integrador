import json
import time
from pyspark.ml.fpm import FPGrowth
from src.config import SEED, APRIORI_SAMPLE, spark_session
from src.common import processes,sample_processes,save_csv,save_json
from src.part5.transactions import transaction
from src.part5.apriori import apriori


def run(spark):
    df=processes(spark).select('ocid','year','category','method','department','amount_band','competition')
    tx=spark.createDataFrame(df.rdd.map(lambda r:(r.ocid,int(r.year),transaction(r))), 'ocid string, year int, items array<string>').cache(); n=tx.count()
    sample=sample_processes(spark,APRIORI_SAMPLE)
    baskets=[transaction(r) for _,r in sample.iterrows()]
    sdf=spark.createDataFrame([(t,) for t in baskets], 'items array<string>').cache(); sdf.count()
    benchmarks=[]; apriori_rows=[]; full_rows=[]; rules=[]
    for support in [.02,.05,.1]:
        print(f'Asociación: soporte={support}, muestra={len(baskets)}, total={n}',flush=True)
        start=time.perf_counter(); exact=apriori(baskets,support); elapsed=time.perf_counter()-start
        benchmarks.append(dict(algorithm='Apriori',scope='same_sample',support=support,seconds=elapsed,transactions=len(baskets),itemsets=len(exact),equivalent=True))
        apriori_rows.extend(dict(min_support=support,items=json.dumps(sorted(k),ensure_ascii=False),frequency=v,support=v/len(baskets)) for k,v in exact.items())
        start=time.perf_counter(); model=FPGrowth(itemsCol='items',minSupport=support,minConfidence=.5).fit(sdf); found={frozenset(r.items):r.freq for r in model.freqItemsets.collect()}; elapsed=time.perf_counter()-start
        assert found==exact, 'Apriori y FP-Growth difieren sobre la misma muestra'
        benchmarks.append(dict(algorithm='FP-Growth',scope='same_sample',support=support,seconds=elapsed,transactions=len(baskets),itemsets=len(found),equivalent=True))
        start=time.perf_counter(); full=FPGrowth(itemsCol='items',minSupport=support,minConfidence=.5).fit(tx)
        items=full.freqItemsets.collect(); raw_rules=full.associationRules.collect(); elapsed=time.perf_counter()-start
        benchmarks.append(dict(algorithm='FP-Growth',scope='full',support=support,seconds=elapsed,transactions=n,itemsets=len(items),equivalent=None))
        full_rows.extend(dict(min_support=support,items=json.dumps(sorted(r.items),ensure_ascii=False),frequency=r.freq,support=r.freq/n) for r in items)
        if support==.02:
            # Coverage by year uses the full transactions, not just years named in a rule.
            grouped={}
            for row in tx.groupBy('year','items').count().collect():
                grouped.setdefault(row.year,[]).append((set(row.items),row['count']))
            for r in raw_rules:
                ant=set(r.antecedent); con=set(r.consequent)
                if r.lift<=1 or any('sin dato' in t for t in ant|con): continue
                # Avoid purely chronological rules and reverse duplicates in selected cases later.
                for year, entries in [('Todos',sum(grouped.values(),[]))]+[(str(y),v) for y,v in grouped.items()]:
                    total=sum(c for _,c in entries); joint=sum(c for b,c in entries if ant|con<=b); na=sum(c for b,c in entries if ant<=b); nc=sum(c for b,c in entries if con<=b)
                    confidence=joint/na if na else 0.; lift=confidence/(nc/total) if nc else 0.
                    rules.append(dict(year=year,antecedent=' ∧ '.join(sorted(ant)),consequent=' ∧ '.join(sorted(con)),support=joint/total,confidence=confidence,lift=lift,antecedent_size=len(ant),consequent_size=len(con),size=len(ant|con),count=joint,transactions=total))
    save_csv('association/frequent_itemsets_apriori.csv',apriori_rows)
    save_csv('association/frequent_itemsets_fpgrowth.csv',full_rows)
    save_csv('association/rules.csv',rules,['year','antecedent','consequent','support','confidence','lift','antecedent_size','consequent_size','size','count','transactions'])
    selected=[]; used=set()
    for r in sorted([r for r in rules if r['year']=='Todos' and r['support']>=.02 and r['confidence']>=.5 and r['lift']>1 and r['size']<=3 and 'ANIO=' not in r['antecedent']+r['consequent']],key=lambda x:(-x['lift'],-x['support'])):
        key=frozenset((r['antecedent']+' ∧ '+r['consequent']).split(' ∧ '))
        if key in used: continue
        used.add(key)
        selected.append(dict(**r,interpretation=f"Entre procesos con {r['antecedent']}, {r['confidence']:.1%} también tienen {r['consequent']}. La combinación cubre {r['support']:.1%} del conjunto y su lift es {r['lift']:.2f}. Es una asociación descriptiva, no causal."))
        if len(selected)==8: break
    save_json('association/selected_rules.json',{'rules':selected,'required':8,'found':len(selected),'status':'complete' if len(selected)>=8 else 'insufficient_evidence','note':'No se fabrican reglas cuando no hay ocho que superen los criterios.'})
    save_csv('association/algorithm_benchmark.csv',benchmarks)
    tx.unpersist(); sdf.unpersist()

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
