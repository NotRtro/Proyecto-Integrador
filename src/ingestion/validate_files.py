"""Validación de los archivos fuente antes de procesarlos (etapa KDD: selección).

Motor: local (Python estándar). Se lee cada línea una sola vez en streaming para
calcular el hash, contar registros y detectar JSON inválido; no hay agregaciones
que justifiquen Spark y así la validación falla rápido, antes de levantar el clúster.

Artefactos:
    metadata/source_manifest.json   tamaño, SHA-256, registros y procedencia por archivo
    metadata/temporal_coverage.csv  objetos por año efectivo y archivo
    metadata/schema.json            claves de primer nivel y sus tipos
    metadata/repairs_<archivo>.json líneas con varios objetos JSON concatenados
"""
import gzip
import hashlib
import json
from collections import Counter

from src.common import now, save_csv, save_json
from src.config import RAW
from src.ingestion.download import MANIFEST, SOURCE_PAGE, LICENSE
from src.ingestion.json_records import decode_line
from src.processing.model import effective_date


def input_files():
    """Archivos .jsonl o .jsonl.gz en RAW; rechaza el mismo año en ambos formatos."""
    paths = sorted(set(RAW.glob('*.jsonl')) | set(RAW.glob('*.jsonl.gz')))
    if not paths:
        raise FileNotFoundError(f'No hay archivos .jsonl o .jsonl.gz en {RAW}. Ejecutar: python -m src.ingestion.download')
    stems = Counter(p.name.removesuffix('.gz') for p in paths)
    duplicated = [name for name, count in stems.items() if count > 1]
    if duplicated:
        # Leer ambos duplicaría los registros del año antes de la deduplicación por ocid.
        raise ValueError(f'El mismo archivo existe comprimido y sin comprimir: {duplicated}')
    return paths


def open_lines(path):
    return gzip.open(path, 'rt', encoding='utf-8') if path.suffix == '.gz' else path.open(encoding='utf-8')


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def provenance(path, downloads):
    """Procedencia registrada por download.py; si el archivo se copió a mano, se indica."""
    entry = next((d for d in downloads.values() if d.get('file') == path.name), None)
    if entry:
        return dict(provenance='Descargado con src.ingestion.download', source_url=entry['url'],
                    license=entry['license'], published_at=entry['last_modified'], downloaded_at=entry['downloaded_at'])
    return dict(provenance='Copiado manualmente; sin registro de descarga', source_url=SOURCE_PAGE,
                license=LICENSE, published_at=None, downloaded_at=None)


def scan(path, schema):
    """Recorre un archivo y devuelve estadísticas, reparaciones y cobertura por año."""
    stats = Counter()
    repairs = []
    years = Counter()
    min_date = max_date = None
    with open_lines(path) as f:
        for line_number, line in enumerate(f, 1):
            stats['lines'] += 1
            try:
                records = decode_line(line)
            except (ValueError, TypeError):
                stats['invalid_json'] += 1
                continue
            if len(records) > 1:
                stats['recovered_concatenated_lines'] += 1
                repairs.append(dict(file=path.name, line=line_number, objects=len(records),
                                    action='Separación lógica de objetos JSON completos concatenados; original intacto'))
            for record in records:
                stats['records'] += 1
                stats['missing_ocid'] += not record.get('ocid')
                date, _ = effective_date(record)
                years[date[:4] if date else 'sin fecha'] += 1
                if date:
                    min_date = min(min_date or date, date)
                    max_date = max(max_date or date, date)
                for key, value in record.items():
                    schema.setdefault(key, set()).add(type(value).__name__)
    return stats, repairs, years, min_date, max_date


def run():
    downloads = json.loads((RAW / MANIFEST).read_text(encoding='utf-8')) if (RAW / MANIFEST).is_file() else {}
    manifest, coverage, schema = [], [], {}
    for path in input_files():
        print(f'Validando {path.name}', flush=True)
        stats, repairs, years, min_date, max_date = scan(path, schema)
        save_json(f'metadata/repairs_{path.name}.json', repairs)
        manifest.append(dict(
            file=path.name, bytes=path.stat().st_size, sha256=file_sha256(path),
            records=stats['records'], lines=stats['lines'], min_effective_date=min_date, max_effective_date=max_date,
            recovered_concatenated_lines=stats['recovered_concatenated_lines'],
            invalid_json=stats['invalid_json'], missing_ocid=stats['missing_ocid'],
            validated_at=now(), **provenance(path, downloads),
        ))
        coverage.extend(dict(file=path.name, effective_year=y, count=n) for y, n in years.items())
    save_json('metadata/source_manifest.json', manifest)
    save_json('metadata/schema.json', {k: sorted(v) for k, v in schema.items()})
    save_csv('metadata/temporal_coverage.csv', coverage)
    if any(x['invalid_json'] or not x['records'] for x in manifest):
        raise ValueError('Entradas vacías o JSON inválido; consultar metadata/source_manifest.json')
    return manifest


if __name__ == '__main__':
    run()
