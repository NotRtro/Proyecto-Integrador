import gzip
import hashlib
import json
from collections import Counter
from src.config import RAW, YEARS
from src.common import save_json, save_csv, now
from src.processing.model import effective_date
from src.ingestion.json_records import decode_line


def input_files():
    paths = sorted(set(RAW.glob('*.jsonl')) | set(RAW.glob('*.jsonl.gz')))
    if not paths:
        raise FileNotFoundError(f'No hay archivos .jsonl o .jsonl.gz en {RAW}')
    return paths


def open_lines(path):
    return gzip.open(path, 'rt', encoding='utf-8') if path.suffix == '.gz' else path.open(encoding='utf-8')


def run():
    manifest, coverage, schema = [], [], {}
    for p in input_files():
        print(f'Validando {p.name}', flush=True)
        digest = hashlib.sha256()
        with p.open('rb') as f:
            for chunk in iter(lambda: f.read(8*1024*1024), b''):
                digest.update(chunk)
        total = invalid = missing = lines = repaired = 0
        repairs = []
        dates = Counter()
        min_date = max_date = None
        with open_lines(p) as f:
            for line in f:
                lines += 1
                try:
                    records = decode_line(line)
                    if len(records)>1:
                        repaired += 1
                        repairs.append(dict(file=p.name,line=lines,objects=len(records),action='Separación lógica de objetos JSON completos concatenados; original intacto'))
                    for r in records:
                        total += 1
                        date, _ = effective_date(r)
                        dates[date[:4] if date else 'sin fecha'] += 1
                        if date:
                            min_date = min(min_date,date) if min_date else date
                            max_date = max(max_date,date) if max_date else date
                        missing += not bool(r.get('ocid'))
                        for k, v in r.items():
                            schema.setdefault(k, set()).add(type(v).__name__)
                except (ValueError, TypeError):
                    invalid += 1
        save_json(f'metadata/repairs_{p.name}.json', repairs)
        manifest.append(dict(file=p.name, bytes=p.stat().st_size, sha256=digest.hexdigest(), records=total, min_effective_date=min_date, max_effective_date=max_date, lines=lines, recovered_concatenated_lines=repaired, invalid_json=invalid, missing_ocid=missing, validated_at=now(), provenance='Archivo local proporcionado por el usuario', license='CC BY 4.0 según plan', source_url='https://data.open-contracting.org/es/publication/135'))
        coverage.extend(dict(file=p.name, effective_year=y, count=n) for y,n in dates.items())
    save_json('metadata/source_manifest.json', manifest)
    save_json('metadata/input_counts.json', manifest)
    save_json('metadata/schema.json', {k: sorted(v) for k,v in schema.items()})
    save_csv('metadata/temporal_coverage.csv', coverage)
    if any(x['invalid_json'] or not x['records'] for x in manifest):
        raise ValueError('Entradas vacías o JSON inválido; consultar manifiesto')
    return manifest

if __name__ == '__main__':
    run()
