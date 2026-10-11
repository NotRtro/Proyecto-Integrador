"""API de solo lectura sobre los artefactos que produce el pipeline.

Motor: FastAPI + pandas sobre archivos pequeños ya agregados por Spark. La API no
recalcula métricas oficiales: filtra, ordena y recorta. Lo único que se calcula aquí es
exploración barata sobre los cubos (totales y concentración de la selección que el
usuario filtra), rotulado como tal en la respuesta.

Cada endpoint es también una función Python normal (parámetros con `Annotated` y valor
por defecto): api/export_static.py la llama directamente para generar la versión
estática de la web, así la lógica no se duplica en JavaScript.
"""

from typing import Annotated, Literal

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.store import apply_filters, document, frame, options, records

app = FastAPI(
    title="Observatorio OECE API",
    version="2.0.0",
    description="API de artefactos analíticos de contrataciones públicas del Perú (OCDS, OECE).",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501", "http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

Metric = Literal["processes", "tender_amount_pen", "award_amount_pen"]


def safe(build):
    try:
        return build()
    except FileNotFoundError as exc:
        raise HTTPException(503, f"Artefacto no generado: {exc.args[0]}") from exc


def optional_document(name, default=None):
    try:
        return document(name)
    except FileNotFoundError:
        return default


def optional_records(name, filters=None):
    try:
        data = frame(name)
    except FileNotFoundError:
        return []
    return records(apply_filters(data, filters or {}))


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/meta")
def meta():
    """Años, totales y fuente: lo que la web no debe tener escrito en el código."""

    def build():
        years = sorted(int(y) for y in frame("eda/sql_yearly_totals.csv")["year"])
        quality = document("metadata/quality_summary.json")
        sources = document("metadata/source_manifest.json")
        first = sources[0] if sources else {}
        return {
            "years": years,
            "period": f"{years[0]}–{years[-1]}" if years else "",
            "selected_processes": quality["selected_processes"],
            "raw_records": quality["raw_records"],
            "source": {
                "url": first.get("source_url"),
                "page": "https://data.open-contracting.org/es/publication/135",
                "license": first.get("license"),
                "published_at": first.get("published_at"),
                "downloaded_at": first.get("downloaded_at"),
                "files": len(sources),
            },
        }

    return safe(build)


@app.get("/api/overview")
def overview():
    def build():
        selected = document("association/selected_rules.json")
        lsh = frame("similarity/lsh_parameter_grid.csv")
        ann_method = document("ann/methodology.json")
        concentration = pd.DataFrame(document("eda/concentration_metrics.json"))
        return {
            "quality": document("metadata/quality_summary.json"),
            "run": document("metadata/run_manifest.json"),
            "validation": document("metadata/artifact_validation.json"),
            "yearly": records(frame("eda/sql_yearly_totals.csv")),
            "selected_rules": selected["found"],
            "best_lsh": records(lsh.sort_values(["f1", "recall"], ascending=False).head(1))[0],
            "best_ann": ann_method["best_hnsw"],
            "best_ivf": ann_method["best_ivf"],
            "full_corpus_lsh": document("similarity/methodology.json").get("full_corpus"),
            "concentration": records(concentration),
            "buyer_hhi": records(frame("eda/buyer_hhi_summary.csv")),
            "single_bidder": records(apply_filters(frame("eda/single_bidder_rates.csv"), {"dimension": "buyer_level"})),
        }

    return safe(build)


@app.get("/api/quality")
def quality():
    return safe(
        lambda: {
            "quality": document("metadata/quality_summary.json"),
            "sources": document("metadata/source_manifest.json"),
            "coverage": records(frame("metadata/temporal_coverage.csv")),
            "nulls": records(frame("metadata/nulls_by_year.csv")),
            "schema": document("metadata/analytical_schema.json"),
            "stopwords": optional_document("metadata/domain_stopwords.json"),
            "processing_timings": optional_document("metadata/processing_timings.json", []),
            "decisions": optional_document("metadata/data_decisions.json"),
        }
    )


@app.get("/api/eda/options")
def eda_options():
    return safe(
        lambda: options(
            frame("eda/filter_cube.csv"),
            [
                "year",
                "month",
                "department",
                "category",
                "modality",
                "buyer_level",
            ],
        )
    )


@app.get("/api/eda")
def eda(
    year: str | None = None,
    month: str | None = None,
    department: str | None = None,
    category: str | None = None,
    modality: str | None = None,
    metric: Metric = "award_amount_pen",
    limit: Annotated[int, Query(ge=5, le=50)] = 10,
):
    """Exploración del cubo filtrado. Los totales, rankings y la concentración se recalculan
    sobre la selección del usuario (exploración en vivo); las cifras oficiales están en
    /api/concentration."""

    def build():
        filters = {"year": year, "month": month, "department": department, "category": category, "modality": modality}
        data = apply_filters(frame("eda/filter_cube.csv"), filters)
        suppliers = apply_filters(frame("eda/supplier_cube.csv"), {k: v for k, v in filters.items() if k != "modality"})
        totals = {c: float(data[c].sum()) for c in ["processes", "tender_amount_pen", "award_amount_pen"]}

        def grouped(columns, source=data, value=metric, top=None):
            result = source.groupby(columns, dropna=False)[value].sum().reset_index()
            if top:
                result = result.sort_values(value, ascending=False).head(top)
            return records(result)

        buyers = data.groupby(["buyer_id", "buyer_name"], dropna=False)[metric].sum().sort_values(ascending=False)
        total = float(buyers.sum())
        shares = buyers / total if total else buyers * 0
        cumulative = shares.cumsum().head(100)
        supplier_metric = "processes" if metric == "processes" else "allocated_amount_pen"
        return {
            "filters": filters,
            "metric": metric,
            "totals": totals,
            "top10_share": float(shares.head(10).sum()),
            "hhi": float((shares**2).sum() * 10000),
            "live_note": "Concentración de la selección filtrada, calculada en vivo sobre el cubo.",
            "monthly": grouped(["year", "month"]),
            "categories": grouped(["category"], top=limit),
            "modalities": grouped(["modality"], top=limit),
            "departments": grouped(["department"], top=limit),
            "levels": grouped(["buyer_level"]),
            "buyers": grouped(["buyer_id", "buyer_name"], top=limit),
            "suppliers": grouped(["supplier_id", "supplier_name", "supplier_type"], suppliers, supplier_metric, limit),
            "supplier_metric": supplier_metric,
            "concentration": [{"rank": i + 1, "share": float(v)} for i, v in enumerate(cumulative)],
        }

    return safe(build)


@app.get("/api/eda/technical")
def eda_technical():
    return safe(
        lambda: {
            "benchmark": records(frame("eda/rdd_dataframe_benchmark.csv")),
            "code": optional_document("eda/code_snippets.json", {}),
            "partitioning": optional_document("eda/partitioning_experiment.json"),
            "spark_vs_pandas": optional_records("eda/spark_vs_pandas.csv"),
            "amount_distribution": records(frame("eda/amount_distribution.csv")),
            "histogram": optional_records("eda/amount_histogram.csv"),
            "percentiles": optional_records("eda/amount_percentiles.csv"),
            "outliers": records(frame("eda/amount_outlier_summary.csv")),
            "timings": optional_document("eda/part1_timings.json", []),
        }
    )


@app.get("/api/concentration")
def concentration(
    year: str | None = None,
    buyer_level: str | None = None,
    limit: Annotated[int, Query(ge=5, le=100)] = 25,
):
    """Q1: concentración oficial (Spark) a nivel nacional, por entidad y por departamento.

    Las tablas por departamento, entidad y ranking son anuales: sin año se muestra el último.
    """

    def build():
        departments = frame("eda/department_supplier_hhi.csv")
        shown = year or str(departments["year"].max())
        buyer_hhi = apply_filters(frame("eda/buyer_supplier_hhi.csv"), {"year": shown, "buyer_level": buyer_level})
        return {
            "year_shown": shown,
            "market": document("eda/concentration_metrics.json"),
            "buyer_summary": records(frame("eda/buyer_hhi_summary.csv")),
            "departments": records(apply_filters(departments, {"year": shown})),
            "most_dependent": records(buyer_hhi.sort_values(["hhi", "total"], ascending=False).head(limit)),
            "top_buyers": records(apply_filters(frame("eda/top_buyers.csv"), {"year": shown}).head(limit)),
            "top_suppliers": records(apply_filters(frame("eda/top_suppliers.csv"), {"year": shown}).head(limit)),
            "repeat_winners": optional_records("streaming/repeat_winners.csv"),
            "supplier_types": document("metadata/quality_summary.json").get("suppliers"),
        }

    return safe(build)


@app.get("/api/competition")
def competition(dimension: str = "modality", year: str = "Todos"):
    """Q2: tasa de postor único por dimensión, reglas dirigidas y ventanas DGIM."""

    def build():
        rates = frame("eda/single_bidder_rates.csv")
        dgim = frame("streaming/dgim_grid.csv")
        series = frame("streaming/error_over_time.csv")
        return {
            "dimensions": sorted(rates["dimension"].unique().tolist()),
            # "Todos" es un valor real de la columna year (tasa global), no la ausencia de filtro.
            "rates": records(
                rates[(rates["dimension"] == dimension) & (rates["year"].astype(str) == year)].sort_values(
                    "single_bidder_rate", ascending=False
                )
            ),
            "by_year": records(rates[(rates["dimension"] == dimension) & (rates["year"].astype(str) != "Todos")]),
            "rules": optional_records("association/single_bidder_rules.csv"),
            "dgim": records(dgim[dgim.signal == "postor_unico"]),
            "dgim_series": records(series[series.signal == "postor_unico"]),
            "bidders": optional_records("eda/bidders_by_modality.csv"),
        }

    return safe(build)


@app.get("/api/similarity")
def similarity():
    """Parte II y Q3: rejilla LSH, error de MinHash, curva S, corpus completo y repetidos."""

    def build():
        grid = frame("similarity/lsh_parameter_grid.csv")
        return {
            "grid": records(grid),
            "default_config": grid.sort_values(["f1", "recall"], ascending=False).iloc[0]["config"],
            "minhash_error": optional_records("similarity/minhash_error.csv"),
            "minhash_scatter": optional_records("similarity/minhash_scatter.csv"),
            "s_curve": optional_records("similarity/s_curve_observed.csv"),
            "templates": optional_records("similarity/template_groups.csv"),
            "repeated_summary": optional_records("similarity/repeated_summary.csv"),
            "repeated_top_buyers": optional_records("similarity/repeated_top_buyers.csv"),
            "case_candidates": optional_document("similarity/case_candidates.json", {}),
            "methodology": document("similarity/methodology.json"),
        }

    return safe(build)


@app.get("/api/similarity/pairs")
def similarity_pairs(
    config: str, status: Literal["TP", "FP", "FN"] = "TP", limit: Annotated[int, Query(ge=1, le=200)] = 25
):
    """Pares de la muestra de evaluación para una configuración y un estado, con sus textos."""

    def build():
        pairs = apply_filters(frame("similarity/similar_pairs.csv"), {"config": config, "status": status})
        pairs = pairs.sort_values("jaccard", ascending=False)
        docs = frame("similarity/documents.csv").set_index("ocid")["text"].to_dict()
        shown = records(pairs.head(limit))
        for pair in shown:
            pair["left_text"] = docs.get(pair["left_ocid"], "")
            pair["right_text"] = docs.get(pair["right_ocid"], "")
        return {"config": config, "status": status, "pairs": shown, "total_pairs": len(pairs)}

    return safe(build)


@app.get("/api/ann")
def ann():
    """Parte III: rejillas HNSW e IVF, escalamiento, metodología y consultas disponibles."""

    def build():
        configs = sorted(
            frame("ann/neighbors_display.csv")["config"].unique().tolist(), key=lambda c: (c != "exact", c)
        )
        return {
            "queries": document("ann/queries.json"),
            "configs": configs,
            "hnsw": records(frame("ann/hnsw_grid.csv")),
            "ivf": optional_records("ann/ivf_grid.csv"),
            "scaling": optional_records("ann/scaling.csv"),
            "methodology": optional_document("ann/methodology.json", {}),
        }

    return safe(build)


@app.get("/api/ann/neighbors")
def ann_neighbors(query: str, config: str = "exact"):
    """Los 10 vecinos precalculados de una consulta con una configuración."""

    def build():
        neighbors = apply_filters(frame("ann/neighbors_display.csv"), {"query_ocid": query, "config": config})
        docs = frame("ann/documents.csv").set_index("ocid")["text"].to_dict()
        result = records(neighbors.sort_values("rank"))
        for item in result:
            item["text"] = docs.get(item["neighbor_ocid"], "")
        return {"query": query, "config": config, "neighbors": result}

    return safe(build)


@app.get("/api/streaming")
def streaming():
    return safe(
        lambda: {
            "bloom": records(frame("streaming/bloom_grid.csv")),
            "bloom_series": optional_records("streaming/bloom_over_time.csv"),
            "cms": records(frame("streaming/cms_grid.csv")),
            "cms_top": optional_records("streaming/cms_top_keys.csv"),
            "dgim": records(frame("streaming/dgim_grid.csv")),
            "dgim_series": records(frame("streaming/error_over_time.csv")),
            "memory": records(frame("streaming/memory_comparison.csv")),
            "repeat_winners": optional_records("streaming/repeat_winners.csv"),
            "live": optional_document("streaming/live_params.json", {}),
            "methodology": document("streaming/methodology.json"),
        }
    )


@app.get("/api/association")
def association(year: str = "Todos", kind: str | None = None):
    """Todas las reglas del año (soporte >= 2 %, confianza >= 50 %); la web las filtra en vivo."""

    def build():
        rules = frame("association/rules.csv")
        # "Todos" es un valor real de rules.csv (métricas globales), no la ausencia de filtro.
        rules = apply_filters(rules[rules["year"].astype(str) == year], {"kind": kind})
        return {
            "rules": records(rules.sort_values(["lift", "support"], ascending=False)),
            "total_rules": len(rules),
            "selected": document("association/selected_rules.json"),
            "single_bidder_rules": optional_records("association/single_bidder_rules.csv"),
            "missing": optional_records("association/missing_report.csv"),
            "benchmark": records(frame("association/algorithm_benchmark.csv")),
        }

    return safe(build)


@app.get("/api/discussion")
def discussion():
    """Evidencia para la discusión crítica: tiempos por etapa, Spark frente a pandas y escalamiento."""
    return safe(
        lambda: {
            "run": document("metadata/run_manifest.json"),
            "processing_timings": optional_document("metadata/processing_timings.json", []),
            "part_timings": {
                name: optional_document(path, [])
                for name, path in [
                    ("part1", "eda/part1_timings.json"),
                    ("part2", "similarity/part2_timings.json"),
                    ("part3", "ann/part3_timings.json"),
                    ("part5", "association/part5_timings.json"),
                ]
            },
            "spark_vs_pandas": optional_records("eda/spark_vs_pandas.csv"),
            "ann_scaling": optional_records("ann/scaling.csv"),
            "lsh_full_corpus": optional_document("similarity/methodology.json", {}).get("full_corpus"),
            "validation": document("metadata/artifact_validation.json"),
            "quality": document("metadata/quality_summary.json"),
        }
    )
