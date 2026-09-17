import time
import statistics
from pyspark.sql import functions as F
from src.config import PROCESSED, spark_session
from src.common import processes, save_csv, save_json
from src.part1 import aggregations_dataframe as df_ops, aggregations_rdd as rdd_ops


def run(spark):
    df = processes(spark).cache(); df.count()
    dims = ['year','month','department','category','buyer_id','buyer_name','competition']
    cube = df.groupBy(*dims).agg(F.count('*').alias('processes'), F.sum('tender_amount_pen').alias('tender_amount_pen'), F.sum('award_amount_pen').alias('award_amount_pen'))
    save_csv('eda/filter_cube.csv', cube.toPandas())
    for name, keys in [('monthly_evolution',['year','month']), ('category_distribution',['year','category']), ('top_buyers',['year','buyer_id','buyer_name']), ('department_distribution',['year','department']), ('competition_distribution',['year','competition'])]:
        save_csv(f'eda/{name}.csv', df.groupBy(*keys).agg(F.count('*').alias('processes'),F.sum('tender_amount_pen').alias('tender_amount_pen'),F.sum('award_amount_pen').alias('award_amount_pen')).toPandas())
    suppliers = spark.read.parquet(str(PROCESSED/'suppliers'))
    supplier_cube = suppliers.join(df.select('ocid','category','department','buyer_name'), 'ocid').groupBy('year','month','department','category','buyer_id','buyer_name','supplier_id','supplier_name').agg(F.sum('allocated_amount_pen').alias('allocated_amount_pen'),F.countDistinct('ocid').alias('processes')).toPandas()
    save_csv('eda/supplier_cube.csv', supplier_cube)
    save_csv('eda/top_suppliers.csv', supplier_cube.groupby(['year','supplier_id','supplier_name'], dropna=False)[['allocated_amount_pen','processes']].sum().reset_index())
    save_csv('eda/buyer_supplier_counts.csv', suppliers.groupBy('year','buyer_id').agg(F.countDistinct('supplier_id').alias('unique_suppliers')).toPandas())
    save_csv('eda/amount_distribution.csv', df.groupBy('year','amount_band').count().toPandas())
    metrics=[]
    for kind, data, key, value in [('buyers',cube.toPandas(),'buyer_id','award_amount_pen'),('suppliers',supplier_cube,'supplier_id','allocated_amount_pen')]:
        for year, group in data.groupby('year'):
            v=group.groupby(key)[value].sum().sort_values(ascending=False); total=float(v.sum())
            shares=v/total if total else v*0
            metrics.append(dict(kind=kind,year=int(year),total=total,top10_share=float(shares.head(10).sum()),hhi=float((shares**2).sum()*10000)))
    save_json('eda/concentration_metrics.json',metrics)
    df.createOrReplaceTempView('process_features')
    sql_totals=spark.sql("SELECT year, COUNT(*) AS processes, SUM(tender_amount_pen) AS tender_amount_pen, SUM(award_amount_pen) AS award_amount_pen FROM process_features GROUP BY year ORDER BY year")
    save_csv('eda/sql_yearly_totals.csv',sql_totals.toPandas())
    results=[]; plans={'SQL_yearly':sql_totals._jdf.queryExecution().toString()}
    for op in ['category_amount','buyer_count']:
        reference=None
        for engine, module in [('DataFrame',df_ops),('RDD',rdd_ops)]:
            begin=time.perf_counter(); query=getattr(module,op)(df); out=query.collect(); warm=time.perf_counter()-begin
            normalized={r[0]: float(r[1] or 0) for r in out}
            if reference is None: reference=normalized
            else:
                assert reference.keys()==normalized.keys()
                assert all(abs(reference[k]-normalized[k]) <= 1e-6*max(1,abs(reference[k])) for k in reference)
            times=[]
            for repetition in range(3):
                start=time.perf_counter(); getattr(module,op)(df).collect(); times.append(time.perf_counter()-start)
            plan=query._jdf.queryExecution().toString() if engine=='DataFrame' else query.toDebugString().decode()
            plans[f'{op}_{engine}']=plan
            results.append(dict(operation=op,engine=engine,warmup_seconds=warm,median_seconds=statistics.median(times),run1=times[0],run2=times[1],run3=times[2],partitions=df.rdd.getNumPartitions(),shuffle='Exchange / reduceByKey',equivalent=True))
    save_csv('eda/rdd_dataframe_benchmark.csv',results); save_json('eda/execution_plans.json',plans)
    df.unpersist()

if __name__ == '__main__':
    s=spark_session()
    try: run(s)
    finally: s.stop()
