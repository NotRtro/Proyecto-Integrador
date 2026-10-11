"""Exporta la web en modo estático: las respuestas de la API como archivos JSON.

Motor: local. Se llaman las mismas funciones de api/main.py con una rejilla de
parámetros, así la versión publicada (GitHub Pages, sin servidor) muestra exactamente
lo mismo que la API.

Estructura: <salida>/<endpoint>/index.json mapea la consulta canónica (parámetros
ordenados, sin vacíos ni "Todos") al archivo con su respuesta. El frontend
(frontend/src/api.js) construye la misma clave. Los filtros que no están en la rejilla
(categoría y mes en el EDA, por ejemplo) solo funcionan en modo local con la API.

Uso:
    ARTIFACTS_PATH=artifacts python -m api.export_static frontend/public/data
"""

import json
import shutil
import sys
from itertools import product
from pathlib import Path

from fastapi import HTTPException

from api import main
from api.store import frame


def canonical(params):
    clean = {k: v for k, v in params.items() if v not in (None, '', 'Todos')}
    return '&'.join(f'{k}={clean[k]}' for k in sorted(clean))


def grid(**axes):
    names = list(axes)
    return [dict(zip(names, values)) for values in product(*axes.values())]


def specs():
    cube = frame('eda/filter_cube.csv')
    years = [None] + sorted(str(y) for y in cube.year.unique())
    departments = [None] + sorted(str(d) for d in cube.department.dropna().unique())
    levels = [None] + sorted(str(level) for level in cube.buyer_level.dropna().unique())
    competition = main.competition()
    lsh = main.similarity()['grid']
    ann = main.ann()
    return {
        'meta': (main.meta, [{}], []),
        'overview': (main.overview, [{}], []),
        'quality': (main.quality, [{}], []),
        'eda/options': (main.eda_options, [{}], []),
        'eda/technical': (main.eda_technical, [{}], []),
        'eda': (
            main.eda,
            grid(
                year=years,
                department=departments,
                metric=['processes', 'tender_amount_pen', 'award_amount_pen'],
                limit=[25],
            ),
            ['year', 'department', 'metric', 'limit'],
        ),
        'concentration': (main.concentration, grid(year=years, buyer_level=levels), ['year', 'buyer_level']),
        'competition': (
            main.competition,
            grid(dimension=competition['dimensions'], year=['Todos'] + years[1:]),
            ['dimension', 'year'],
        ),
        'integrity': (main.integrity, grid(dimension=main.integrity()['dimensions']), ['dimension']),
        'similarity': (main.similarity, [{}], []),
        'similarity/pairs': (
            main.similarity_pairs,
            grid(config=[r['config'] for r in lsh], status=['TP', 'FP', 'FN']),
            ['config', 'status'],
        ),
        'ann': (main.ann, [{}], []),
        'ann/neighbors': (
            main.ann_neighbors,
            grid(query=[q['ocid'] for q in ann['queries']], config=ann['configs']),
            ['query', 'config'],
        ),
        'streaming': (main.streaming, [{}], []),
        'association': (main.association, grid(year=['Todos'] + years[1:]), ['year']),
        'discussion': (main.discussion, [{}], []),
    }


def export(output):
    output = Path(output)
    if output.exists():
        shutil.rmtree(output)
    total = 0
    for endpoint, (function, combinations, supported) in specs().items():
        folder = output / endpoint
        folder.mkdir(parents=True, exist_ok=True)
        files = {}
        for number, params in enumerate(combinations):
            try:
                response = function(**params)
            except HTTPException as exc:
                print(f'  omitido {endpoint} {params}: {exc.detail}', flush=True)
                continue
            name = f'{number}.json'
            (folder / name).write_text(
                json.dumps(response, ensure_ascii=False, default=str, allow_nan=False), encoding='utf-8'
            )
            files[canonical(params)] = name
        (folder / 'index.json').write_text(
            json.dumps(dict(params=supported, files=files), ensure_ascii=False), encoding='utf-8'
        )
        total += len(files)
        print(f'{endpoint}: {len(files)} respuestas', flush=True)
    size = sum(p.stat().st_size for p in output.rglob('*.json'))
    print(f'Total: {total} respuestas, {size / 1e6:.1f} MB en {output}', flush=True)


if __name__ == '__main__':
    export(sys.argv[1] if len(sys.argv) > 1 else 'frontend/public/data')
