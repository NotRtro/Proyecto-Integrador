from typing import Literal

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.store import apply_filters, document, frame, options, records


app = FastAPI(
    title="Observatorio OECE API",
    version="1.0.0",
    description="API de artefactos analíticos de contrataciones públicas del Perú.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def safe(callable_):
    try:
        return callable_()
    except FileNotFoundError as exc:
        raise HTTPException(503, f"Artefacto no generado: {exc.args[0]}") from exc


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/overview")
def overview():
    def build():
        quality = document("metadata/quality_summary.json")
        run = document("metadata/run_manifest.json")
        validation = document("metadata/artifact_validation.json")
        yearly = frame("eda/sql_yearly_totals.csv")
        selected = document("association/selected_rules.json")
        best_lsh = frame("similarity/lsh_parameter_grid.csv").sort_values(
            ["f1", "recall", "precision"], ascending=False
        ).iloc[0]
        ann_grid = frame("ann/hnsw_grid.csv")
        high_recall = ann_grid[ann_grid.recall_at_10 >= 0.95]
        best_ann = (high_recall if not high_recall.empty else ann_grid).sort_values(
            ["speedup", "recall_at_10"], ascending=False
        ).iloc[0]
        return {
            "quality": quality,
            "run": run,
            "validation": validation,
            "yearly": records(yearly),
            "selected_rules": selected["found"],
            "best_lsh": records(pd.DataFrame([best_lsh]))[0],
            "best_ann": records(pd.DataFrame([best_ann]))[0],
        }
    return safe(build)


@app.get("/api/quality")
def quality():
    return safe(lambda: {
        "quality": document("metadata/quality_summary.json"),
        "sources": document("metadata/source_manifest.json"),
        "coverage": records(frame("metadata/temporal_coverage.csv")),
        "nulls": records(frame("metadata/nulls_by_year.csv")),
        "schema": document("metadata/analytical_schema.json"),
    })


@app.get("/api/eda/options")
def eda_options():
    return safe(lambda: options(frame("eda/filter_cube.csv"), [
        "year", "month", "department", "category", "buyer_name", "competition"
    ]))


@app.get("/api/eda")
def eda(
    year: str | None = None,
    month: str | None = None,
    department: str | None = None,
    category: str | None = None,
    buyer_name: str | None = None,
    metric: Literal["processes", "tender_amount_pen", "award_amount_pen"] = "award_amount_pen",
    limit: int = Query(10, ge=5, le=50),
):
    def build():
        filters = locals_filters = {
            "year": year, "month": month, "department": department,
            "category": category, "buyer_name": buyer_name,
        }
        data = apply_filters(frame("eda/filter_cube.csv"), locals_filters)
        suppliers = apply_filters(frame("eda/supplier_cube.csv"), filters)
        totals = {column: float(data[column].sum()) for column in [
            "processes", "tender_amount_pen", "award_amount_pen"
        ]}

        def grouped(columns, source=data, value=metric, top=None):
            grouped_data = source.groupby(columns, dropna=False)[value].sum().reset_index()
            if top:
                grouped_data = grouped_data.sort_values(value, ascending=False).head(top)
            return records(grouped_data)

        buyers = data.groupby(["buyer_id", "buyer_name"], dropna=False)[metric].sum().sort_values(ascending=False)
        total = float(buyers.sum())
        top10_share = float(buyers.head(10).sum() / total) if total else 0.0
        shares = buyers / total if total else buyers * 0
        concentration = []
        accumulated = 0.0
        for rank, value in enumerate(shares.tolist(), 1):
            accumulated += float(value)
            if rank <= 100:
                concentration.append({"rank": rank, "share": accumulated})
        supplier_value = "allocated_amount_pen"
        return {
            "filters": filters,
            "metric": metric,
            "totals": totals,
            "top10_share": top10_share,
            "hhi": float((shares ** 2).sum() * 10000),
            "monthly": grouped(["year", "month"]),
            "categories": grouped(["category"], top=limit),
            "departments": grouped(["department"], top=limit),
            "buyers": grouped(["buyer_id", "buyer_name"], top=limit),
            "suppliers": grouped(["supplier_id", "supplier_name"], suppliers, supplier_value, limit),
            "concentration": concentration,
        }
    return safe(build)


@app.get("/api/eda/technical")
def eda_technical():
    return safe(lambda: {
        "benchmark": records(frame("eda/rdd_dataframe_benchmark.csv")),
        "plans": document("eda/execution_plans.json"),
        "amount_distribution": records(frame("eda/amount_distribution.csv")),
        "concentration": document("eda/concentration_metrics.json"),
    })


@app.get("/api/similarity")
def similarity(
    config: str | None = None,
    status: Literal["TP", "FP", "FN"] | None = None,
    year: str | None = None,
    category: str | None = None,
    limit: int = Query(25, ge=1, le=200),
):
    def build():
        grid = frame("similarity/lsh_parameter_grid.csv")
        selected_config = config or grid.sort_values(["f1", "recall"], ascending=False).iloc[0]["config"]
        pairs = frame("similarity/similar_pairs.csv")
        pairs = apply_filters(pairs, {
            "config": selected_config, "status": status,
            "left_year": year, "left_category": category,
        }).sort_values("jaccard", ascending=False)
        docs = frame("similarity/documents.csv").set_index("ocid")["text"].to_dict()
        shown = records(pairs.head(limit))
        for pair in shown:
            pair["left_text"] = docs.get(pair["left_ocid"], "")
            pair["right_text"] = docs.get(pair["right_ocid"], "")
        return {
            "grid": records(grid),
            "selected": records(grid[grid.config == selected_config].head(1)),
            "pairs": shown,
            "total_pairs": len(pairs),
            "review": document("similarity/reviewed_cases.json"),
            "methodology": document("similarity/methodology.json"),
        }
    return safe(build)


@app.get("/api/ann")
def ann(
    query: str | None = None,
    config: str = "exact",
    k: int = Query(5, ge=1, le=10),
    search: str | None = None,
):
    def build():
        grid = frame("ann/hnsw_grid.csv")
        queries = document("ann/queries.json")
        if search:
            term = search.casefold()
            queries_filtered = [q for q in queries if term in q["text"].casefold() or term in q["ocid"].casefold()]
        else:
            queries_filtered = queries
        query_id = query or (queries_filtered[0]["ocid"] if queries_filtered else None)
        neighbors = frame("ann/neighbors_display.csv")
        neighbors = apply_filters(neighbors, {"query_ocid": query_id, "config": config})
        neighbors = neighbors[neighbors["rank"] <= k]
        docs = frame("ann/documents.csv").set_index("ocid")["text"].to_dict()
        result = records(neighbors)
        for item in result:
            item["text"] = docs.get(item["neighbor_ocid"], "")
        query_item = next((q for q in queries if q["ocid"] == query_id), None)
        return {
            "grid": records(grid), "queries": queries_filtered[:40],
            "query": query_item, "config": config, "neighbors": result,
            "ivf": document("ann/ivf_status.json"),
        }
    return safe(build)


@app.get("/api/streaming")
def streaming():
    return safe(lambda: {
        "bloom": records(frame("streaming/bloom_grid.csv")),
        "cms": records(frame("streaming/cms_grid.csv")),
        "dgim": records(frame("streaming/dgim_grid.csv")),
        "error_over_time": records(frame("streaming/error_over_time.csv")),
        "memory": records(frame("streaming/memory_comparison.csv")),
        "methodology": document("streaming/methodology.json"),
    })


@app.get("/api/association")
def association(
    year: str = "Todos",
    support: float = Query(0.02, ge=0, le=1),
    confidence: float = Query(0.5, ge=0, le=1),
    lift: float = Query(1.01, ge=0),
    antecedent: str | None = None,
    size: int = Query(4, ge=2, le=6),
    limit: int = Query(50, ge=1, le=500),
):
    def build():
        rules = frame("association/rules.csv")
        rules = rules[
            (rules.year.astype(str) == year) & (rules.support >= support) &
            (rules.confidence >= confidence) & (rules.lift >= lift) &
            (rules["size"] <= size)
        ]
        if antecedent:
            rules = rules[rules.antecedent.str.contains(antecedent, regex=False)]
        rules = rules.sort_values(["lift", "support"], ascending=False)
        return {
            "rules": records(rules.head(limit)), "total_rules": len(rules),
            "selected": document("association/selected_rules.json"),
            "benchmark": records(frame("association/algorithm_benchmark.csv")),
        }
    return safe(build)
