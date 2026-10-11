# Minería de contrataciones públicas del Perú (OCDS · OECE)

Proyecto integrador de Minería de Datos Masivos. Analiza los procesos de contratación pública publicados por el OECE en estándar OCDS con Apache Spark, técnicas de similitud aproximada, búsqueda de vecinos, minería de flujos y reglas de asociación. Los resultados se presentan en un reporte web interactivo.

- **Fuente:** [Registro de Datos OCDS del OECE, publicación 135](https://data.open-contracting.org/es/publication/135). Archivos anuales `.jsonl.gz` de 2023, 2024 y 2025 (unos 330 MB comprimidos y 279 mil registros). Licencia [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- **Equipo:** Rodo Vilcarromero · Ronaldo Flores · Isaac Vera.

## Preguntas de interés

| | Pregunta | Técnicas que la responden |
|---|---|---|
| **Q1** | ¿Qué tan concentrada está la adjudicación en pocos proveedores dentro de cada entidad y departamento, y cambió entre años? | HHI con funciones de ventana en Spark (Parte I), Bloom Filter sobre pares entidad–proveedor (Parte IV) |
| **Q2** | ¿Qué características del proceso se asocian a tener un solo postor? | Tasas por dimensión en Spark (Parte I), FP-Growth dirigido (Parte V), DGIM (Parte IV) |
| **Q3** | ¿Hay requerimientos casi idénticos convocados repetidamente por una entidad, o por muchas a la vez? | Shingling, MinHash y LSH sobre todo el corpus (Parte II); HNSW e IVF (Parte III) |

Count-Min Sketch (Parte IV) estima frecuencias de proveedores y de códigos UNSPSC. La Parte VI (discusión) está en la propia web.

## Ejecutar

Requisitos: Docker Desktop con unos 8 GB de RAM y Docker Compose v2. No hace falta Python ni Java en el anfitrión.

```bash
make all      # build + clúster + descarga + pipeline completo (~27 min la primera vez)
```

Sin `make` (por ejemplo, Git Bash en Windows):

```bash
docker compose build
docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
docker compose run --rm --no-deps pipeline python -m src.ingestion.download
docker compose run --rm pipeline python -m src.run_all
```

Después, abrir **http://localhost:8501** (web), **http://localhost:8000/docs** (API) y **http://localhost:8080** (Spark).

`data/raw`, `data/processed` y `artifacts` persisten en el anfitrión. La descarga es reanudable y no se repite si el archivo coincide en tamaño y fecha de publicación. `data/raw/download_manifest.json` registra la URL, `Last-Modified`, el tamaño y el SHA-256 de cada archivo. El portal se actualiza cada mes, así que ese manifiesto identifica la versión exacta de los datos.

### Perfiles, mediciones y cachés

Lo que responde Q1, Q2 y Q3 usa siempre **todos** los datos; los perfiles solo cambian la evaluación de ANN y si se repiten las mediciones de una vez. Las decisiones y su evidencia (qué se pierde al muestrear con cada diseño de muestra y cuánto cuesta no hacerlo) están en la web, en *Datos y KDD → Decisiones de volumen*, y en `artifacts/metadata/data_decisions.json`.

| Modo | Comando | Tiempo medido |
|---|---|---|
| Completo (informe y web publicada) | `make run` | ~25–27 min |
| Rápido con caché (trabajo diario, todos los datos) | `make rapido` | ~8 min |
| Muestra de desarrollo (probar código) | `make dev` | ~9 min |

Opciones de `src.run_all`: `--profile completo|rapido`, `--experiments si|no|auto` (mediciones de una vez: benchmark RDD/DataFrame, particionamiento, Spark vs. pandas, escalamiento ANN, efecto de muestrear la Q1) y `--refresh` (ignorar las cachés de validación, staging y procesamiento). `artifacts/metadata/experiments.json` dice cuándo y con qué código se midió cada experimento.

### Modo muestra (desarrollo)

Una muestra reproducible del 3 % de las líneas permite probar el pipeline completo de punta a punta. Sus cifras **no** valen para el informe; para trabajar con resultados reales conviene `make rapido`, que con caché tarda lo mismo.

```bash
make dev-sample && make dev
# sin make:
docker compose run --rm --no-deps pipeline python scripts/make_dev_sample.py
export DATA_DIR=./data/dev ARTIFACTS_DIR=./data/dev/artifacts
docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
docker compose run --rm pipeline python -m src.run_all
```

### Otros comandos

```bash
make test                                              # pytest (incluye Spark), API y frontend
make notebook                                          # genera y ejecuta notebooks/sustento.ipynb
cd frontend && npm ci && npm test                      # tests de render de la web (vitest)
docker compose run --rm pipeline python -m src.run_all --from-stage part2   # reanudar desde una etapa
docker compose run --rm pipeline python -m src.part3.run                      # una parte sola
```

En Git Bash, anteponer `MSYS_NO_PATHCONV=1` a los comandos que pasan rutas como `/data/...`, para que no se conviertan en rutas de Windows.

## Publicar la web

La web publicada es estática: React más los JSON que `api/export_static.py` genera llamando a las mismas funciones de la API. No necesita servidor.

```bash
make static                        # exporta frontend/public/data (unos 12 MB, versionado en git)
cd frontend && VITE_STATIC=1 VITE_BASE=/Proyecto-Integrador/ npm run build
```

`.github/workflows/pages.yml` hace ese build y lo publica en GitHub Pages en cada push a `main`. Hay que activar Pages una vez en *Settings → Pages → Source: GitHub Actions*. En la versión publicada, la categoría, el mes y la modalidad del EDA se filtran solo en modo local. Todo lo demás funciona igual que en local.

## Estructura y motor de cada etapa

| Módulo | Motor | Por qué |
|---|---|---|
| `src/ingestion/download.py`, `validate_files.py` | local | Transferencia HTTP y validación en streaming: falla rápido, antes de levantar el clúster |
| `src/processing/build_processes.py` | **Spark** | Parseo de ~3 GB de JSON anidado, deduplicación, conversión de monedas y escritura en Parquet particionado |
| `src/processing/model.py`, `entity_level.py`, `normalize_text.py` | Python dentro de Spark | Funciones puras que Spark ejecuta en paralelo por documento |
| `src/processing/text_features.py` | **Spark SQL** | Stopwords por frecuencia documental y shingles sin UDF de Python |
| `src/part1/` (EDA, concentración, RDD vs. DataFrame, particionamiento) | **Spark** | Exigido por el enunciado; solo se traen al driver resultados agregados |
| `src/part1/spark_vs_pandas.py` | Spark y local | Compara ambos motores en la misma agregación |
| `src/part1/participation.py` | **Spark** | Frecuencia de ítems (segmentos UNSPSC) y participación: quién participa, quién gana y quién nunca gana |
| `src/part2/full_corpus.py`, `repeated.py` | **Spark** | LSH sobre los 196 mil procesos sin comparar todos los pares |
| `src/part2/groups.py` | Spark + local | Une los pares en grupos (componentes conexas) y los etiqueta con las palabras más distintivas |
| `src/part2/evaluation.py` | local + Spark | La verdad exacta de la muestra (X·Xᵀ disperso) cabe en el driver; los candidatos LSH se calculan en Spark |
| `src/part3/` | local | TF-IDF, SVD y los índices faiss viven en la memoria de una máquina; las consultas tardan microsegundos |
| `src/part4/` | Spark + local | Spark ordena el flujo; las estructuras procesan un evento a la vez, como en tiempo real |
| `src/part4/replay.py` | **Spark Structured Streaming** | Reproduce el flujo una semana por micro-lote (fuente de archivos, `foreachBatch`, checkpoint) |
| `src/part5/` | **Spark** + local | FP-Growth (MLlib) sobre todas las canastas y sobre los segmentos UNSPSC que se compran juntos; Apriori propio sobre una muestra como referencia |
| `scripts/build_notebook.py` | local | Genera `notebooks/sustento.ipynb`: un gráfico y una comprobación automática por afirmación |
| `src/export/web_artifacts.py` | local | 19 controles de consistencia antes de servir la web |
| `api/` | FastAPI | Filtra artefactos precalculados; no recalcula métricas oficiales |
| `frontend/` | React | Reporte por pregunta; controles que recalculan en vivo las curvas teóricas |

Cada módulo explica en su docstring el motor que usa y el motivo, sus entradas y sus artefactos.

## Datos y criterios

- **Periodo:** el año sale de la fecha de convocatoria (`tender.datePublished`; si falta, `tenderPeriod.startDate`), no del archivo. Los archivos anuales también traen procesos antiguos recompilados.
- **Duplicados:** se conserva la versión más reciente de cada `ocid` (`release.date`, con desempate por archivo).
- **Montos:** se usa `amount_PEN` de la fuente; si no está, el monto en PEN nativo, la tasa implícita de la convocatoria o la mediana histórica de las tasas OECE (diaria, anual o del periodo). Los faltantes nunca se convierten en cero. El monto de una adjudicación se reparte en partes iguales entre sus proveedores.
- **Texto:** se toman la descripción del requerimiento y de sus ítems. El título OCDS es la nomenclatura del proceso (`AS-SM-12-2024-MDCH/CS-1`), así que va aparte como `tender_code`. Las stopwords del dominio (frecuencia documental > 30 %) se calculan a partir de los datos.
- **Modalidad:** se agrupa desde `procurementMethodDetails`, porque `procurementMethod` vale "open" en el 82 % de los procesos.
- **Nivel de gobierno:** se infiere del nombre de la entidad con reglas ordenadas (99,6 % clasificado). El OCDS no publica este dato.
- **Proveedores:** se usa un nombre canónico por RUC y un tipo según el prefijo (persona natural, persona jurídica, consorcio, extranjero). Los consorcios reciben un identificador de un solo uso y ocultan a sus integrantes.
- **Departamento:** es el domicilio del comprador, no el lugar de ejecución.
- **Privacidad:** no se publican teléfonos ni contactos, ni rankings de personas naturales.

## Resultados y verificación

- `artifacts/metadata/`: manifiestos de fuente y de corrida (con el commit del código), calidad, esquema, stopwords y tiempos de procesamiento.
- `artifacts/{eda,similarity,ann,streaming,association}/`: resultados de cada parte.
- `artifacts/metadata/artifact_validation.json`: 19 controles, entre ellos conservación de montos, garantías de Bloom, CMS y DGIM, FPR de Bloom igual al teórico con la misma ocupación, equivalencia Apriori = FP-Growth y MinHash que se acerca a la teoría.
- `notebooks/sustento.ipynb`: el sustento de cada parte con gráficos y comprobaciones automáticas (✔/✘) contra la teoría o la versión exacta; se regenera con `make notebook`.
- `tests/`: 66 tests de pytest (algoritmos e integración con Spark). `frontend/src/__tests__`: render de todas las páginas con datos reales y completitud del contenido escrito.

## Interpretaciones y casos

El texto de análisis de la web vive en `frontend/src/content` y se versiona con el código:

- `hallazgos/*.md`: la interpretación de cada uno de los 30 hallazgos (pregunta, cifras, contexto peruano y límites).
- `rules.json`: la interpretación de cada regla seleccionada (clave `rule_id`).
- `similarity_cases.json`: casos de similitud revisados en el registro OCDS completo de ambos procesos, con su nomenclatura para ubicarlos en el SEACE.

`frontend/src/__tests__/content.test.js` falla si un hallazgo queda sin interpretación o si el pipeline selecciona una regla que no tiene la suya. Si se vuelve a correr el pipeline con datos nuevos, hay que revisar las cifras citadas en estos textos.
