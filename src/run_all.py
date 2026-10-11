"""Orquestador del pipeline completo (etapas en orden, con manifiesto de la corrida).

Opciones de volumen y tiempo (docs/DecisionesTecnicas.md):
    --profile completo|rapido   rapido evalúa ANN sobre 50 mil procesos; Q1-Q3 siempre con todo
    --experiments si|no|auto    mediciones de una vez: repetir, omitir o solo si faltan
    --refresh                   ignorar las cachés de validación y de staging
"""

import argparse
import hashlib
import importlib
import importlib.metadata
import os
import subprocess
import time
import traceback
from pathlib import Path

from src.common import now, save_json
from src.config import (
    APRIORI_SAMPLE,
    EXPERIMENT_MODES,
    PROFILES,
    SAMPLE,
    SEED,
    YEARS,
    experiments_mode,
    profile_settings,
    run_profile,
    spark_session,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--from-stage',
        choices=['validation', 'processing', 'part1', 'part2', 'part3', 'part4', 'part5', 'export'],
        default='validation',
    )
    parser.add_argument(
        '--profile', choices=list(PROFILES), help='Perfil de ejecución (por defecto RUN_PROFILE o completo)'
    )
    parser.add_argument(
        '--experiments', choices=EXPERIMENT_MODES, help='Mediciones de una vez (por defecto según el perfil)'
    )
    parser.add_argument('--refresh', action='store_true', help='Ignorar las cachés de validación y staging')
    args = parser.parse_args()
    # Las etapas leen estas opciones del entorno en tiempo de ejecución (src/config.py).
    if args.profile:
        os.environ['RUN_PROFILE'] = args.profile
    if args.experiments:
        os.environ['RUN_EXPERIMENTS'] = args.experiments
    if args.refresh:
        os.environ['RUN_REFRESH'] = '1'
    stages = (
        [('validation', 'src.ingestion.validate_files'), ('processing', 'src.processing.build_processes')]
        + [(f'part{i}', f'src.part{i}.run') for i in range(1, 6)]
        + [('export', 'src.export.web_artifacts')]
    )
    try:
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], stderr=subprocess.DEVNULL, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = os.getenv('CODE_VERSION', 'uncommitted')
    code_hash = hashlib.sha256()
    for path in sorted(Path('src').rglob('*.py')):
        code_hash.update(str(path).encode())
        code_hash.update(path.read_bytes())
    manifest = dict(
        code_sha256=code_hash.hexdigest(),
        dependencies={
            p: importlib.metadata.version(p) for p in ['pyspark', 'numpy', 'pandas', 'faiss-cpu', 'scikit-learn']
        },
        started_at=now(),
        status='running',
        code_version=revision,
        years=YEARS,
        seed=SEED,
        sample_size=SAMPLE,
        apriori_sample=APRIORI_SAMPLE,
        profile=run_profile(),
        profile_settings=profile_settings(),
        experiments=experiments_mode(),
        refresh=os.getenv('RUN_REFRESH') == '1',
        stages=[],
    )
    spark = None
    enabled = False
    try:
        for name, module in stages:
            enabled = enabled or name == args.from_stage
            if not enabled:
                continue
            print(f'\n>>> ETAPA {name}', flush=True)
            start = time.perf_counter()
            manifest['current_stage'] = name
            save_json('metadata/run_manifest.json', manifest)
            runner = importlib.import_module(module).run
            if name in ['validation', 'export']:
                runner()
            else:
                if spark is None:
                    spark = spark_session()
                runner(spark)
            manifest['stages'].append(dict(name=name, seconds=time.perf_counter() - start, status='complete'))
            save_json('metadata/run_manifest.json', manifest)
        manifest['status'] = 'complete'
    except Exception as exc:
        manifest['status'] = 'failed'
        manifest['error'] = str(exc)
        manifest['traceback'] = traceback.format_exc()
        raise
    finally:
        manifest['finished_at'] = now()
        save_json('metadata/run_manifest.json', manifest)
        if spark:
            spark.stop()


if __name__ == '__main__':
    main()
