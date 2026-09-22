import json
import urllib.parse
import urllib.request


BASE = "http://api:8000"


def get(path, params=None):
    query = "?" + urllib.parse.urlencode(params) if params else ""
    with urllib.request.urlopen(BASE + path + query, timeout=60) as response:
        assert response.status == 200
        return json.load(response)


assert get("/api/health") == {"status": "ok"}
overview = get("/api/overview")
assert overview["quality"]["selected_processes"] > 0
quality = get("/api/quality")
assert len(quality["sources"]) == 3
options = get("/api/eda/options")
assert {2023, 2024, 2025}.issubset(set(options["year"]))
eda = get("/api/eda", {"year": 2024, "metric": "processes", "limit": 5})
assert eda["totals"]["processes"] > 0 and len(eda["buyers"]) <= 5
similarity = get("/api/similarity", {"status": "TP", "limit": 3})
assert len(similarity["grid"]) == 36
ann = get("/api/ann", {"k": 5})
assert len(ann["neighbors"]) == 5
streaming = get("/api/streaming")
assert all(row["fn"] == 0 for row in streaming["bloom"])
association = get("/api/association", {"support": .02, "confidence": .5})
assert association["selected"]["found"] == 8
print("PASS API: 9 endpoints y filtros verificados")
