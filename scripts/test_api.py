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
assert all(overview["validation"]["checks"].values())

quality = get("/api/quality")
assert len(quality["sources"]) == meta["source"]["files"] >= 1 and quality["stopwords"]["terms"]
assert quality["decisions"]["tasks"], "falta la tabla de decisiones de volumen"

options = get("/api/eda/options")
assert set(years).issubset(set(options["year"])) and options["modality"]
eda = get("/api/eda", {"year": years[-1], "metric": "processes", "limit": 5})
assert eda["totals"]["processes"] > 0 and len(eda["buyers"]) <= 5 and eda["modalities"]
technical = get("/api/eda/technical")
assert technical["benchmark"] and technical["partitioning"]["pruning"]["rows"] > 0

concentration = get("/api/concentration", {"year": years[-1]})
assert all(0 <= r["hhi"] <= 10000 for r in concentration["market"])

competition = get("/api/competition", {"dimension": "modality"})
assert competition["rates"] and all(between(r["single_bidder_rate"]) for r in competition["rates"])
assert competition["adjusted"] and all(between(r["adjusted_rate"]) for r in competition["adjusted"])
assert competition["persistence"]["comparable_strata"] >= competition["persistence"]["strata_where_direct_is_higher"]

integrity = get("/api/integrity", {"dimension": "modality"})
assert integrity["by_dimension"] and all(between(r["rate"]) for r in integrity["summary"])
assert all(r["flagged"] <= r["evaluable"] <= r["processes"] for r in integrity["summary"])
assert sum(r["processes"] for r in integrity["counts"]) == meta["selected_processes"]

similarity = get("/api/similarity")
assert similarity["grid"] and similarity["minhash_error"]
assert all(between(r["precision"]) and between(r["recall"]) for r in similarity["grid"])
pairs = get("/api/similarity/pairs", {"config": similarity["default_config"], "status": "TP", "limit": 3})
assert len(pairs["pairs"]) <= 3

ann = get("/api/ann")
assert ann["hnsw"] and ann["ivf"] and all(between(r["recall_at_10"]) for r in ann["hnsw"] + ann["ivf"])
neighbors = get("/api/ann/neighbors", {"query": ann["queries"][0]["ocid"], "config": "exact"})
assert 1 <= len(neighbors["neighbors"]) <= 10

streaming = get("/api/streaming")
assert all(row["fn"] == 0 for row in streaming["bloom"]), "Bloom no puede tener falsos negativos"
assert all(row["underestimates"] == 0 for row in streaming["cms"]), "CMS no puede subestimar"

association = get("/api/association")
selected = association["selected"]
assert selected["found"] >= 8 or selected["status"] == "insufficient_evidence"
assert all(r["support"] >= 0.02 and r["confidence"] >= 0.5 for r in association["rules"])

discussion = get("/api/discussion")
assert discussion["run"]["status"] == "complete"

print(f"PASS API: 16 endpoints verificados para {meta['period']}")
