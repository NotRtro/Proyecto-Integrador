import argparse
import hashlib
from pathlib import Path
import importlib.metadata
import importlib
import os
import subprocess
import time
import traceback
from src.config import YEARS,SEED,SAMPLE,ANN_SAMPLE,APRIORI_SAMPLE,spark_session
from src.common import save_json,now

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--from-stage',choices=['validation','processing','part1','part2','part3','part4','part5','export'],default='validation'); args=parser.parse_args()
    stages=[('validation','src.ingestion.validate_files'),('processing','src.processing.build_processes')]+[(f'part{i}',f'src.part{i}.run') for i in range(1,6)]+[('export','src.export.web_artifacts')]
    try: revision=subprocess.check_output(['git','rev-parse','HEAD'],stderr=subprocess.DEVNULL,text=True).strip()
    except (OSError,subprocess.CalledProcessError): revision=os.getenv('CODE_VERSION','uncommitted')
    code_hash=hashlib.sha256()
    for path in sorted(Path('src').rglob('*.py')):
        code_hash.update(str(path).encode()); code_hash.update(path.read_bytes())
    manifest=dict(code_sha256=code_hash.hexdigest(), dependencies={p:importlib.metadata.version(p) for p in ['pyspark','numpy','pandas','hnswlib','scikit-learn']}, started_at=now(),status='running',code_version=revision,years=YEARS,seed=SEED,sample_size=SAMPLE,ann_sample=ANN_SAMPLE,apriori_sample=APRIORI_SAMPLE,stages=[])
    spark=None; enabled=False
    try:
        for name,module in stages:
            enabled=enabled or name==args.from_stage
            if not enabled: continue
            print(f'\n>>> ETAPA {name}',flush=True); start=time.perf_counter()
            manifest['current_stage']=name; save_json('metadata/run_manifest.json',manifest)
            runner=importlib.import_module(module).run
            if name in ['validation','export']: runner()
            else:
                if spark is None: spark=spark_session()
                runner(spark)
            manifest['stages'].append(dict(name=name,seconds=time.perf_counter()-start,status='complete'))
            save_json('metadata/run_manifest.json',manifest)
        manifest['status']='complete'
    except Exception as exc:
        manifest['status']='failed'; manifest['error']=str(exc); manifest['traceback']=traceback.format_exc(); raise
    finally:
        manifest['finished_at']=now(); save_json('metadata/run_manifest.json',manifest)
        if spark: spark.stop()

if __name__=='__main__': main()
