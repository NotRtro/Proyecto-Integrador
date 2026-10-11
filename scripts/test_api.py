"""Prueba de humo de la API contra los artefactos generados.

Valida invariantes (rangos, no vacío, coherencia entre endpoints) en lugar de
cantidades exactas, para que el test siga siendo válido si cambian los datos del
portal, la muestra o las rejillas de parámetros.
"""
import json
import urllib.parse
import urllib.request

BASE = "http://api:8000"


def get(path, params=None):
    query = "?" + urllib.parse.urlencode(params) if params else ""
    with urllib.request.urlopen(BASE + path + query, timeout=60) as response:
        assert response.status == 200, path
        return json.load(response)


def between(value, low=0.0, high=1.0):
    return value is None or low <= value <= high


assert get("/api/health") == {"status": "ok"}

meta = get("/api/meta")
years = meta["years"]
assert years and meta["selected_processes"] > 0 and meta["raw_records"] >= meta["selected_processes"]

overview = get("/api/overview")
assert overview["quality"]["selected_processes"] == meta["selected_processes"]

quality = get("/api/quality")
assert len(quality["sources"]) == meta["source"]["files"] >= 1

options = get("/api/eda/options")
assert set(years).issubset(set(options["year"]))
eda = get("/api/eda", {"year": years[-1], "metric": "processes", "limit": 5})
assert eda["totals"]["processes"] > 0 and len(eda["buyers"]) <= 5
assert between(eda["top10_share"]) and 0 <= eda["hhi"] <= 10000

similarity = get("/api/similarity", {"status": "TP", "limit": 3})
assert similarity["grid"] and len(similarity["pairs"]) <= 3
assert all(between(r["precision"]) and between(r["recall"]) for r in similarity["grid"])

ann = get("/api/ann", {"k": 5})
assert 1 <= len(ann["neighbors"]) <= 5
assert all(between(r["recall_at_10"]) for r in ann["grid"])

streaming = get("/api/streaming")
assert all(row["fn"] == 0 for row in streaming["bloom"]), "Bloom no puede tener falsos negativos"
assert all(row["estimate"] >= row["exact"] for row in streaming["cms"]), "CMS no puede subestimar"

association = get("/api/association", {"support": .02, "confidence": .5})
selected = association["selected"]
assert selected["found"] >= 8 or selected["status"] == "insufficient_evidence"
assert all(r["support"] >= .02 and r["confidence"] >= .5 for r in association["rules"])

print(f"PASS API: 10 endpoints verificados para {meta['period']}")
