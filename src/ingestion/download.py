"""Descarga de los archivos anuales OCDS del OECE (Registro de Datos OCDS, publicación 135).

Motor: local (Python estándar). Es una transferencia HTTP secuencial de tres archivos;
Spark no aporta nada aquí.

Fuente: https://data.open-contracting.org/es/publication/135 (licencia CC BY 4.0).
El portal se actualiza mensualmente, por eso cada descarga registra la fecha
Last-Modified, el tamaño y el SHA-256 en `data/raw/download_manifest.json`: así
se sabe exactamente qué versión de los datos produjo los resultados.

Uso:
    python -m src.ingestion.download            # años de ANALYSIS_YEARS
    python -m src.ingestion.download --force    # vuelve a descargar aunque exista
"""

import argparse
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from src.config import RAW, YEARS

SOURCE_PAGE = 'https://data.open-contracting.org/es/publication/135'
URL_TEMPLATE = os.getenv(
    'OCDS_DOWNLOAD_URL',
    'https://data.open-contracting.org/es/publication/135/download?name={year}.jsonl.gz',
)
LICENSE = 'CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/)'
MANIFEST = 'download_manifest.json'
CHUNK = 1024 * 1024
RETRIES = 5


def target_path(year):
    return RAW / f'peru_oece_bulk_{year}.jsonl.gz'


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8 * CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def remote_info(url):
    """Sigue la redirección al CDN y devuelve URL final, tamaño y fecha de publicación."""
    request = urllib.request.Request(url, method='HEAD')
    with urllib.request.urlopen(request, timeout=60) as response:
        return dict(
            final_url=response.geturl(),
            bytes=int(response.headers['Content-Length']),
            last_modified=response.headers.get('Last-Modified'),
        )


def fetch(url, destination, expected_bytes):
    """Descarga a un archivo `.part`, reanudando con Range si una conexión se corta."""
    partial = destination.with_name(destination.name + '.part')
    for attempt in range(1, RETRIES + 1):
        offset = partial.stat().st_size if partial.exists() else 0
        if offset > expected_bytes:
            partial.unlink()
            offset = 0
        if offset == expected_bytes:
            break
        request = urllib.request.Request(url, headers={'Range': f'bytes={offset}-'} if offset else {})
        try:
            with urllib.request.urlopen(request, timeout=120) as response, partial.open('ab') as out:
                if offset and response.status != 206:
                    # El servidor ignoró Range: se reinicia desde cero para no corromper el archivo.
                    out.truncate(0)
                    offset = 0
                received = offset
                last_report = time.monotonic()
                for chunk in iter(lambda: response.read(CHUNK), b''):
                    out.write(chunk)
                    received += len(chunk)
                    if time.monotonic() - last_report > 5:
                        print(f'  {destination.name}: {received / expected_bytes:.0%}', flush=True)
                        last_report = time.monotonic()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            wait = 2**attempt
            print(f'  Intento {attempt}/{RETRIES} falló ({exc}); reintento en {wait}s', flush=True)
            time.sleep(wait)
    size = partial.stat().st_size if partial.exists() else 0
    if size != expected_bytes:
        raise RuntimeError(f'{destination.name}: se recibieron {size} de {expected_bytes} bytes')
    os.replace(partial, destination)


def load_manifest():
    path = RAW / MANIFEST
    return json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}


def run(years=None, force=False):
    RAW.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    for year in years or YEARS:
        url = URL_TEMPLATE.format(year=year)
        destination = target_path(year)
        info = remote_info(url)
        previous = manifest.get(str(year), {})
        up_to_date = (
            destination.is_file()
            and destination.stat().st_size == info['bytes']
            and previous.get('last_modified') == info['last_modified']
        )
        if up_to_date and not force:
            print(f'{destination.name}: ya descargado ({info["last_modified"]})', flush=True)
            continue
        print(f'Descargando {destination.name} ({info["bytes"] / 1e6:.0f} MB)', flush=True)
        fetch(info['final_url'], destination, info['bytes'])
        manifest[str(year)] = dict(
            file=destination.name,
            source_page=SOURCE_PAGE,
            url=url,
            final_url=info['final_url'],
            license=LICENSE,
            bytes=info['bytes'],
            sha256=sha256(destination),
            last_modified=info['last_modified'],
            downloaded_at=datetime.now(timezone.utc).isoformat(),
        )
        (RAW / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--years', help='Años separados por coma; por defecto ANALYSIS_YEARS')
    parser.add_argument('--force', action='store_true', help='Descargar aunque el archivo esté al día')
    args = parser.parse_args()
    run([int(y) for y in args.years.split(',')] if args.years else None, args.force)
