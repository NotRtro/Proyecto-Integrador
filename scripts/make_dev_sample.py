"""Crea una muestra pequeña de los archivos fuente para iterar rápido (modo desarrollo).

Motor: local. Lee cada archivo en streaming y conserva una línea de cada `1/fraction`,
elegida por un hash estable del número de línea y la semilla. Así la muestra cubre
todo el archivo (no solo el inicio, que puede estar ordenado por fecha u ocid) y es
reproducible entre corridas.

La muestra sirve para probar que el pipeline corre de punta a punta; sus cifras no
deben usarse en el informe.

Uso:
    python scripts/make_dev_sample.py                    # 3 % por defecto -> data/dev/raw
    python scripts/make_dev_sample.py --fraction 0.01
    make dev-sample && make dev                          # lo mismo en Docker
Luego:
    RAW_PATH=data/dev/raw PROCESSED_PATH=data/dev/processed ARTIFACTS_PATH=data/dev/artifacts python -m src.run_all
"""

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import RAW, SEED  # noqa: E402
from src.ingestion.validate_files import input_files, open_lines  # noqa: E402


def keep(line_number, fraction):
    digest = hashlib.blake2b(f'{SEED}:{line_number}'.encode(), digest_size=8).digest()
    return int.from_bytes(digest, 'little') / 2**64 < fraction


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--fraction', type=float, default=0.03)
    # Junto a RAW (data/raw -> data/dev/raw) para que funcione igual dentro y fuera de Docker.
    parser.add_argument('--output', type=Path, default=RAW.parent / 'dev' / 'raw')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    summary = {}
    for source in input_files():
        name = source.name if source.suffix == '.gz' else source.name + '.gz'
        kept = total = 0
        with open_lines(source) as lines, gzip.open(args.output / name, 'wt', encoding='utf-8') as out:
            for line_number, line in enumerate(lines, 1):
                total += 1
                if keep(line_number, args.fraction):
                    out.write(line)
                    kept += 1
        summary[name] = dict(source=source.name, lines=total, kept=kept)
        print(f'{name}: {kept:,} de {total:,} líneas', flush=True)
    (args.output / 'dev_sample.json').write_text(
        json.dumps(dict(fraction=args.fraction, seed=SEED, files=summary), indent=2), encoding='utf-8'
    )


if __name__ == '__main__':
    main()
