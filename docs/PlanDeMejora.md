# Plan de mejora: código y lógica

Fuente de los puntos: [EvaluacionYMejoras.md](EvaluacionYMejoras.md). Cada paso cita los IDs que resuelve (G-, I-, II-, III-, IV-, V-, VI-, C-, B-).

## 1. Alcance

**Dentro del plan:** todo lo que se resuelve con código o lógica: pipeline Spark, algoritmos, artefactos, API, frontend, tests, Docker y README.

**Fuera del plan (lo hace el equipo, con apoyo del código):**

| ID | Tarea del equipo | Apoyo que deja el código |
|---|---|---|
| I-05, V-01, VI-06, VI-02 | Redactar interpretaciones con contexto peruano y la discusión crítica | Archivos de contenido editables (`frontend/src/content/`) que la web muestra en cada hallazgo, más las cifras reales en los artefactos |
| II-04 | Validar a mano los casos de similitud en SEACE | Lista de pares candidatos con enlace al `ocid` y estado `pendiente` / `validado` |
| VI-01 (video) | Grabar la demostración | Web publicada y guion actualizado |
| G-02 | Completar el documento de selección | Cifras reales de registros y tamaños en `source_manifest.json` |
| C-09 | Preparar la sustentación oral | Docstrings y código legible |

## 2. Forma de trabajo

1. **Rama de trabajo.** Todo se hace en una rama (`mejoras`), con **un commit por paso** de este plan para poder revisar y revertir.
2. **Un paso a la vez.** Se implementa, se prueba, se marca `[x]` en este archivo y recién entonces se pasa al siguiente.
3. **Criterio de aceptación.** Cada paso tiene una condición verificable. No se marca hecho sin cumplirla.
4. **Modo muestra para iterar.** Correr el pipeline completo tarda mucho, así que se itera con un subconjunto pequeño (paso 0.2) y se hace una corrida completa al final de cada fase.
5. **Calidad en el camino.** Cada módulo que se toca se deja con docstring Spark/local (C-01), formateado y sin líneas con `;` (C-02). No se deja para el final.

### Decisiones tomadas (10/10/2026)

| Tema | Decisión |
|---|---|
| Datos | Se descargan del portal OCDS con `download.py` (paso 0.1) |
| Publicación | Web **estática sin API** (GitHub Pages o Vercel); la API queda para uso local (paso 7.3) |
| Git | Rama `mejoras`, un commit por paso; al final, PR a `main` |
| Preguntas de interés | Q1 concentración de proveedores · Q2 postor único · Q3 requerimientos repetidos ([EvaluacionYMejoras.md §5](EvaluacionYMejoras.md)) |

### Estado del entorno (10/10/2026)

| Recurso | Estado | Implicancia |
|---|---|---|
| Docker 29.7 + Compose v5 | Instalado, **daemon apagado** | Hay que iniciar Docker Desktop antes de la Fase 0 |
| Archivos JSONL 2023–2025 | Descargados en `data/raw/` (327 MB, publicación del 04/10/2026, 279 105 registros) | Listo |
| Python 3.12 y Java 17 locales | Disponibles | Permite correr tests y Spark `local[*]` sin Docker. `hnswlib` y `faiss` pueden no tener wheel en Windows, así que la corrida oficial sigue siendo en Docker |
| Disco libre | 96 GB | Suficiente |

---

## 3. Fases y pasos

El orden sigue las dependencias: los datos alimentan las partes, las partes generan los artefactos y la web los consume.

```
Fase 0 Cimientos ─► Fase 1 Capa de datos ─► Fases 2–6 Partes I–V ─► Fase 7 Export/API ─► Fase 8 Frontend ─► Fase 9 Cierre
```

### Fase 0: Cimientos (sin esto no se puede probar nada)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [x] 0.1 | C-03 | `download.py` descarga de verdad los `.jsonl.gz` 2023–2025 del registro OCDS (publicación 135), con reintentos, reanudación y SHA-256. Los datos van a `data/raw/` y `docker-compose.yml` monta `./data/raw` en lugar de la raíz del repo. | `python -m src.ingestion.download` deja los 3 archivos y `validate_files` pasa |
| [x] 0.2 | — | `scripts/make_dev_sample.py` toma N líneas por año y las deja en `data/raw_dev/`. La variable `RAW_PATH` permite elegir completo o muestra. | `run_all` termina sobre la muestra en menos de 10 min |
| [x] 0.3 | B-01, B-02, B-03, VI-07 | Corregir la carga infinita en Ann y Similarity cuando la API falla. Quitar "198 mil" y los años escritos en el código: un endpoint `/api/meta` entrega años y totales. | Con la API caída, la página muestra el error. No quedan cifras ni años fijos en `frontend/src` |
| [x] 0.4 | B-04, V-05 | `test_api.py` valida invariantes (rangos, no vacío, `found >= 8` o estado `insufficient_evidence`) en lugar de cantidades exactas. | `make test` pasa con artefactos de la muestra |
| [x] 0.5 | — | Corrida base completa con el código actual, guardando una copia de `artifacts/` como referencia para comparar después. | `run_manifest.status == complete` |

**Línea base (0.5, código original, datos completos):** 279 105 registros → 196 277 procesos 2023–2025; ítems 230 496, adjudicaciones 164 712, proveedores 161 255, contratos 159 703; 8 reglas; todos los controles en verde. Tiempos: validation 98 s · processing 908 s · part1 149 s · part2 58 s · part3 91 s · part4 92 s · part5 101 s · export 12 s (≈ 25 min). `artifacts/` pesa 362 MB. Copia en `data/baseline/` (no versionada) para comparar.

### Fase 1: Capa de datos (`src/processing`)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [x] 1.1 | C-08, C-02 | Refactor de `build_processes.py`: tasas como parámetros, funciones con nombre, una sola pasada de parseo a Parquet `staging`, hijos con `explode`, `repartition('year','month')` antes de escribir y tiempos por subetapa en `metadata/processing_timings.json`. **Resultado:** `processing` 908 s → 322 s (2,8×); tablas hijas idénticas a la línea base, montos de procesos con diferencias ≤ 6·10⁻¹⁶ por orden de suma. Causa principal: 288 archivos pequeños por tabla (8 tareas × 36 meses) sobre el volumen montado → 36 archivos. | Test de integración pasa; mismos conteos que la corrida base; `processing` más rápido |
| [ ] 1.2 | V-04, G-03 | Nueva columna `buyer_level` (nacional / regional / local / empresa pública / otro) derivada del nombre de la entidad con reglas documentadas. | Más del 95 % clasificado; el porcentaje queda en `quality_summary.json` |
| [ ] 1.3 | I-08, G-03 | Normalizar proveedores: nombre canónico por `supplier_id` (el más reciente), bandera `is_consortium` y conteo de variantes de nombre. | `quality_summary` reporta cuántos IDs tenían varios nombres |
| [ ] 1.4 | II-06 | Stopwords de dominio: Spark calcula la frecuencia documental y agrega los términos con df > 30 %. Se guardan en `metadata/domain_stopwords.json` y los shingles se generan después. Se conservan los tokens numéricos de un carácter (evita el caso "-1" / "-2"). | Lista publicada; test de normalización actualizado |
| [ ] 1.5 | I-09 | `process_features` deja de ser una copia: pasa a tener solo `ocid`, tokens, shingles, canasta de reglas y atributos de modelado. | Columnas distintas de `processes`; las partes 2–5 la leen |
| [ ] 1.6 | C-01 | Docstrings Spark/local en `ingestion/` y `processing/`. | Todos los módulos tienen cabecera |

### Fase 2: Parte I (EDA, Spark, RDD y particionamiento)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 2.1 | I-02, G-01 (Q1) | HHI, participación del top 10 y rankings calculados **en Spark** con `Window`, por año, departamento, entidad y por **proveedor dentro de cada entidad**. | No queda `groupby` de pandas en `part1/`; nuevo `eda/concentration_*.csv` |
| [ ] 2.2 | I-03, G-03 | `method` entra al cubo, con su distribución por año y categoría. | Artefacto `eda/method_distribution.csv` |
| [ ] 2.3 | I-04 | Histograma log10 de montos y percentiles (p10–p99) por año y categoría con `percentile_approx`. | Artefacto `eda/amount_histogram.csv` |
| [ ] 2.4 | G-01 (Q2) | Tasa de postor único por modalidad, departamento, categoría, banda de monto, nivel de gobierno y año. | Artefacto `eda/single_bidder_rates.csv` |
| [ ] 2.5 | I-06 | RDD vs. DataFrame: se agrega una operación no trivial (HHI proveedor-por-entidad con join) en ambos estilos. Se guardan el código fuente de cada versión, sus líneas, el número real de *Exchange* leído del plan y los tiempos. | `eda/rdd_dataframe_benchmark.csv` con tres operaciones y `code_snippets.json` |
| [ ] 2.6 | I-01 | Experimento de particionamiento: (a) consulta por año con y sin *partition pruning* (`PartitionFilters`, archivos leídos); (b) `shuffle.partitions` = 8, 64 y 200 (tiempo y tareas); (c) `repartition(buyer_id)` frente a ninguno. Un resumen legible del plan reemplaza el volcado crudo. | `eda/partitioning_experiment.json` con las tres comparaciones |
| [ ] 2.7 | G-04 | La misma agregación en Spark y en pandas sobre muestras de 10 %, 50 % y 100 % (tiempo y memoria pico). | `eda/spark_vs_pandas.csv` |
| [ ] 2.8 | C-01, C-02 | Docstrings y formato en `part1/`. | — |

### Fase 3: Parte II (MinHash y LSH)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 3.1 | II-01 | Jaccard estimado por MinHash (`Ĵ` = proporción de componentes iguales) contra el exacto, con k ∈ {16, 32, 64, 128, 256}: MAE, RMSE, sesgo y una muestra de puntos para el gráfico de dispersión. | `similarity/minhash_error.csv`; el RMSE baja aproximadamente como 1/√k |
| [ ] 3.2 | II-03 | Muestra de evaluación por bloques (procesos de las mismas entidades o categorías) para que existan pares positivos. Se reporta `truth_pairs` por umbral. | Al menos 50 pares verdaderos con umbral 0.7 |
| [ ] 3.3 | II-02 | LSH sobre **todo el corpus** en Spark: firmas distribuidas (`mapPartitions`), bandas, candidatos y verificación con Jaccard exacto en Spark. Se limitan y reportan los buckets gigantes que generan las plantillas. | `similarity/full_corpus_summary.json` con N, pares posibles, candidatos, verificados y tiempo |
| [ ] 3.4 | G-01 (Q3) | Sobre los pares verificados: misma entidad en ≤ 30 días (posible reconvocatoria o fraccionamiento) frente a entidades distintas con texto casi idéntico (oportunidad de compra corporativa). | `similarity/repeated_requirements.parquet` y resumen |
| [ ] 3.5 | II-05, II-07, II-08 | Exportar los puntos observados de la rejilla para la curva S (la teórica se calcula en la web). Los tiempos incluyen las firmas y se aclara el motor de cada medición. Justificar en `methodology.json` por qué MinHash y no SimHash. | Campos nuevos en la rejilla y la metodología |
| [ ] 3.6 | II-04 | Reemplazar `case_notes.json` (escrito a mano y etiquetado como IA) por `content/similarity_cases.json`: candidatos elegidos de 3.4, enlace SEACE, campo `estado` y texto vacío para el equipo. | No queda la etiqueta "asistida por IA" |
| [ ] 3.7 | C-01, C-02 | Docstrings y formato en `part2/`. | — |

### Fase 4: Parte III (ANN)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 4.1 | III-04 | 200 consultas, con recall@k reportado como media ± IC 95 % (bootstrap). | Columnas `recall_ci_low` y `recall_ci_high` |
| [ ] 4.2 | III-02 | Escalamiento: TF-IDF → SVD a 256 dimensiones; N ∈ {5k, 20k, 50k, 100k, total}; exacto frente a HNSW (tiempo de consulta y de construcción). | `ann/scaling.csv` muestra dónde se cruzan ANN y exacto |
| [ ] 4.3 | III-03 | IVF con `faiss-cpu` (`IndexIVFFlat`), con `nlist` ∈ {64, 256, 1024} y `nprobe` ∈ {1, 4, 16, 64}. | `ann/ivf_grid.csv`; se elimina `ivf_status.json` |
| [ ] 4.4 | III-05, III-06 | Memoria de la matriz exacta frente a los índices. Justificación de TF-IDF frente a shingles binarios (comparación de recall). | Columnas de memoria; texto en `ann/methodology.json` |
| [ ] 4.5 | C-01, C-02 | Docstrings y formato en `part3/`. | — |

### Fase 5: Parte IV (flujos)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 5.1 | — | El flujo pasa a ser de **adjudicaciones** (evento = proveedor adjudicado con fecha), ordenado por fecha. Así hay proveedores y pares entidad-proveedor. | Conteo de eventos en `methodology.json` |
| [ ] 5.2 | IV-02, G-01 (Q1) | Bloom sobre pares `(buyer_id, supplier_id)`: "¿este proveedor ya ganó antes con esta entidad?". FPR observado frente a teórico. | Decenas de miles de claves insertadas |
| [ ] 5.3 | IV-01, G-03 | CMS sobre `supplier_id` y códigos UNSPSC: error en heavy hitters frente a la cola, y fracción de estimaciones dentro de la cota ε·N. | Error mayor que cero y cota verificada |
| [ ] 5.4 | IV-04, G-01 (Q2) | DGIM con ventana en **días** además de en eventos. Bits: postor único y alto monto. | Test nuevo para la ventana temporal |
| [ ] 5.5 | IV-03 | Exportar n, cardinalidades y distribución exacta de frecuencias para que la web recalcule en vivo el FPR de Bloom (m, k) y la cota de CMS (w, d). | Artefacto `streaming/live_params.json` |
| [ ] 5.6 | IV-05, IV-06, C-01, C-02 | Limpiar `n%max(100,500)`, docstrings y formato. Recomendación por estructura en `methodology.json`. | — |

### Fase 6: Parte V (reglas)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 6.1 | V-04 | Canasta nueva: se agrega `NIVEL=` y `ANIO` sale de la canasta (el año se usa solo para las métricas por año). La justificación queda en la metodología. | Test de `transaction` actualizado |
| [ ] 6.2 | V-02, G-01 (Q2) | Reglas con consecuente fijo `POSTORES=uno` (y `cuatro o mas` como contraste). Se marcan las reglas triviales o normativas (por ejemplo modalidad ↔ banda de monto). | `association/single_bidder_rules.csv` y columna `kind` |
| [ ] 6.3 | V-03 | Reporte de "sin dato" por atributo. Las reglas con "sin dato" pasan a un artefacto aparte en lugar de descartarse. | `association/missing_report.csv` |
| [ ] 6.4 | V-01 | Se elimina la interpretación f-string. Las reglas seleccionadas se enlazan con `content/rules.json`, donde el equipo escribe la interpretación. La lectura automática de métricas se rotula como tal. | Ninguna "interpretación" generada por plantilla |
| [ ] 6.5 | C-01, C-02 | Docstrings y formato en `part5/`. | — |

### Fase 7: Exportación y API

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 7.1 | I-02 | La API deja de calcular métricas oficiales: lee las de Spark. El HHI sobre filtros arbitrarios queda como exploración en vivo (permitido por el enunciado) y se rotula así. | Las cifras de la portada vienen de artefactos Spark |
| [ ] 7.2 | G-01, G-03 | Endpoints nuevos para los artefactos de las Fases 2–6, más filtros de proveedor, rango de monto y modalidad. | OpenAPI actualizado; `test_api.py` los cubre |
| [ ] 7.3 | VI-01, C-05 | Exportación estática: `src/export/static_site.py` escribe JSON ligeros y el frontend tiene un modo `VITE_STATIC=1` que funciona sin API. Se versiona un `artifacts_sample/` menor a 20 MB. | `npm run build` estático se abre sin backend |
| [ ] 7.4 | — | `artifact_validation` verifica los artefactos nuevos (conservación de montos, recall en [0, 1], cota de CMS, etc.). | Todos los controles en verde |

### Fase 8: Frontend (reporte interactivo)

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 8.1 | G-01 | Navegación organizada por pregunta: Inicio · Datos y KDD · **Q1** Concentración · **Q2** Competencia · **Q3** Requerimientos similares · Laboratorio de técnicas · Discusión. Cada sección indica a qué Parte de la rúbrica responde. | Las 5 partes son visibles y rotuladas |
| [ ] 8.2 | VI-03 | Componente `FindingModule` con la anatomía pregunta → pipeline (diagrama con etiqueta Spark/local) → visualización → controles → interpretación. | Todas las páginas lo usan |
| [ ] 8.3 | I-07 | Página "Datos y KDD": cinco etapas, esquema final y limpieza. | — |
| [ ] 8.4 | II-01, II-05, IV-03 | Laboratorio con sliders en vivo: k de MinHash frente al error, curva S con b y r, Bloom con m y k, CMS con w y d, ventana de DGIM y umbrales de reglas. | Al menos 5 controles que recalculan en vivo |
| [ ] 8.5 | III-01 | Explicación conceptual de HNSW e IVF con diagramas SVG y los parámetros que controlan cada compromiso. | — |
| [ ] 8.6 | VI-04, VI-05, VI-08, VI-09 | Prosa en lugar de `JSON.stringify`; pie con la cita completa de la fuente; gráfico de montos en la portada; ranking de proveedores según la métrica elegida. | Ningún `<pre>{JSON.stringify` |
| [ ] 8.7 | I-05, V-01, VI-02, VI-06 | Sistema de contenido: `frontend/src/content/*.md` por hallazgo, más una página de Discusión con las 5 preguntas del enunciado y evidencia automática (tiempos, escalamiento, Spark vs. pandas). Los textos los escribe el equipo. | Los marcadores "pendiente de redacción" se ven en modo desarrollo |
| [ ] 8.8 | C-06 | Tests de render con `vitest` para cada página (con datos simulados). | `npm test` pasa |

### Fase 9: Cierre

| Paso | IDs | Qué se hace | Criterio de aceptación |
|---|---|---|---|
| [ ] 9.1 | C-02 | Pasada final de `ruff format` y `ruff check` sobre `src/`, `api/` y `tests/`. | Sin errores |
| [ ] 9.2 | C-04 | Archivar `PLAN_EJECUCION_OECE_JSON_DOCKER.md` en `docs/historico/`. README actualizado (descarga, modo muestra, modo estático, publicación). | — |
| [ ] 9.3 | C-07 | `CODE_VERSION` como build-arg para el manifiesto. | El manifiesto muestra el commit real |
| [ ] 9.4 | — | Corrida completa final: `make test` y `artifact_validation` en verde. | `status == complete` |
| [ ] 9.5 | VI-01 | Publicar la web estática y dejar el enlace en el README. | URL accesible |

---

## 4. Cobertura de la evaluación

| Grupo | IDs | Pasos |
|---|---|---|
| Transversal | G-01, G-03, G-04 | 2.1, 2.4, 3.4, 8.1 · 1.2, 1.3, 2.2, 5.3, 7.2 · 2.7 |
| Parte I | I-01 … I-09 | 2.6 · 2.1, 7.1 · 2.2 · 2.3 · 8.7 · 2.5 · 8.3 · 1.3 · 1.5 |
| Parte II | II-01 … II-08 | 3.1 · 3.3 · 3.2 · 3.6 · 3.5, 8.4 · 1.4 · 3.5 · 3.5 |
| Parte III | III-01 … III-06 | 8.5 · 4.2 · 4.3 · 4.1 · 4.4 · 4.4 |
| Parte IV | IV-01 … IV-06 | 5.3 · 5.2 · 5.5, 8.4 · 5.4 · 5.6 · 5.6 |
| Parte V | V-01 … V-05 | 6.4 · 6.2 · 6.3 · 6.1 · 0.4 |
| Parte VI | VI-01 … VI-09 | 7.3, 9.5 · 8.7 · 8.2 · 8.6 · 8.6 · 8.7 · 0.3 · 8.6 · 8.6 |
| Código | C-01 … C-08 | 1.6 y cierre de cada fase · cada fase y 9.1 · 0.1 · 9.2 · 7.3 · 8.8 · 9.3 · 1.1 |
| Errores | B-01 … B-04 | 0.3 · 0.3 · 0.3 · 0.4 |

## 5. Riesgos

| Riesgo | Mitigación |
|---|---|
| LSH sobre todo el corpus genera buckets enormes por plantillas comunes | Tope de tamaño por bucket, reportado como limitación; stopwords de dominio (1.4) |
| Escalamiento de ANN a unos 200k vectores consume mucha RAM | SVD a 256 dimensiones en float32 (unos 200 MB) y consultas por lotes |
| `hnswlib` no compila en Windows (verificado); `faiss-cpu` sí instala | Evaluar en la Fase 4 usar faiss para HNSW (`IndexHNSWFlat`) e IVF, lo que permite correr todo también sin Docker |
| La corrida completa tarda horas | Modo muestra (0.2) para iterar; `--from-stage` para reanudar |
| Las cifras cambian con las nuevas reglas y rompen textos ya escritos | Los textos del equipo se escriben **después** de la corrida completa de la Fase 6 |
