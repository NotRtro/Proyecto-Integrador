# Evaluación rigurosa del proyecto y lista de mejoras

**Proyecto:** Minería de contrataciones OECE (OCDS) 2023–2025
**Equipo:** Rodo Vilcarromero · Ronaldo Flores · Isaac Vera
**Referencias evaluadas:** [ProyectoIntegrador1.md](ProyectoIntegrador1.md) (enunciado y rúbrica) y [SeleccionBDPublicaPeru.md](SeleccionBDPublicaPeru.md) (compromisos del equipo)
**Estado evaluado:** rama `main`, commit `34b0191`

> **Alcance de la revisión.** Se leyó todo el código (`src/`, `api/`, `frontend/`, `tests/`, `scripts/`, Docker y README). El repositorio **no contiene** `artifacts/` ni `data/` (están en `.gitignore`), así que no se pudieron verificar los resultados numéricos. La calificación evalúa lo que el código produce y lo que la web muestra. Si el pipeline no corre de punta a punta, la nota baja.

---

## 1. Calificación estimada

| Criterio (rúbrica) | Pts. | Nota estimada | Motivo principal |
|---|---:|---:|---|
| Parte I: dataset, preguntas, KDD/EDA y Spark | 3 | **1.75** | KDD y limpieza muy sólidos. Las preguntas son metodológicas, no de interés. Parte del EDA se calcula en pandas. Falta la distribución por modalidad. No se explica el particionamiento ni su efecto en el plan. La comparación RDD vs. DataFrame se limita a tiempos. |
| Parte II: Jaccard, shingling, MinHash, LSH | 3 | **1.50** | **No se compara la estimación MinHash contra el Jaccard exacto** (requisito explícito). LSH solo corre sobre 600 procesos, no sobre el dataset. El caso interpretado está marcado como "asistido por IA". |
| Parte III: ANN (IVF / HNSW) | 3 | **2.00** | HNSW frente al exacto con una rejilla de parámetros: bien. **Falta la explicación conceptual de IVF y HNSW.** El corpus de 5 000 es demasiado pequeño para que ANN gane. |
| Parte IV: DGIM, Bloom, Count-Min Sketch | 3 | **1.75** | Las tres estructuras son implementación propia y se comparan contra el valor exacto. **CMS sobre 3–4 categorías no demuestra nada.** Bloom usa entidades de baja cardinalidad. Ningún parámetro es interactivo en vivo. |
| Parte V: Apriori / FP-Growth | 3 | **1.75** | Apriori y FP-Growth son equivalentes sobre la misma muestra (bien). **Las "interpretaciones" son una plantilla f-string**, no un análisis en contexto. Las reglas no apuntan a ninguna pregunta de interés. |
| Parte VI: análisis, discusión y reporte web | 4 | **1.50** | La web es pulida, pero la interpretación es genérica y casi no hay contexto peruano. Lo de 10×/100× es una sola oración. Falta discutir ética y transparencia. No hay enlace publicado ni video. |
| Código y reproducibilidad | 1 | **0.60** | Docker, README y tests: bien. El código es denso, con `;` y casi sin comentarios. Ningún módulo declara si corre en Spark o en local y por qué. La descarga no se automatiza. El plan no coincide con el código. |
| **Total** | **20** | **≈ 10.9 / 20** | |

**Diagnóstico general.** La ingeniería es fuerte: Docker con clúster Spark, trazabilidad de montos y monedas, deduplicación por `ocid`, validaciones y tests. Lo que el enunciado más premia está flojo: la **interpretación en contexto peruano**, unas **preguntas de interés propias** y el **análisis de compromisos**. El enunciado advierte: *"Entregar gráficos/tablas/código sin comprenderlos ni saber explicarlos recibirá bajo puntaje, aunque funcionen"*. Hoy la web se lee como un tablero técnico con textos plantilla, no como un informe de hallazgos.

---

## 2. Lo que ya está bien (conservar)

- Ingesta robusta: SHA-256, recuperación de JSON concatenados, manifiesto de fuentes y cobertura temporal ([validate_files.py](../src/ingestion/validate_files.py)).
- Fecha efectiva bien definida (`tender.datePublished` → `tenderPeriod.startDate`) y deduplicación por última `release_date` ([build_processes.py](../src/processing/build_processes.py)).
- Conversión de moneda auditable, con método y tasa por fila. Los nulos nunca se convierten en cero.
- Reparto de montos entre proveedores sin duplicar, con verificación de conservación ([web_artifacts.py](../src/export/web_artifacts.py)).
- Bloom, CMS, DGIM, MinHash y Apriori implementados desde cero, con tests de invariantes ([test_algorithms.py](../tests/test_algorithms.py)).
- Arquitectura "calcular una vez, explorar muchas" respetada (Spark → artefactos → API → React).
- Cautela ética explícita en varios textos ("similitud no es irregularidad").

---

## 3. Observaciones por parte

Severidad: 🔴 **Crítica** (incumple un requisito explícito) · 🟠 **Alta** (afecta mucho la nota) · 🟡 **Media** · ⚪ **Baja**

### 3.0 Transversal: dataset y preguntas de interés

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| G-01 | 🔴 | **Las preguntas no son "de interés propias, no meramente descriptivas".** De las cinco preguntas ([Overview.jsx](../frontend/src/pages/Overview.jsx)), P2–P4 preguntan por la precisión de una técnica ("¿qué precisión obtiene LSH?"). Esas son preguntas metodológicas. P1 y P5 son descriptivas. El enunciado pide 2 o 3 preguntas con relevancia práctica. | Reformular 2 o 3 preguntas de dominio y hacer que **todas las técnicas** respondan a ellas. Hay una propuesta en la §5. |
| G-02 | 🟠 | [SeleccionBDPublicaPeru.md](SeleccionBDPublicaPeru.md) **no tiene el enlace de la fuente** (dice que se citará, pero no la cita). Faltan el número de registros y el tamaño descomprimido (el README menciona unos 3 GB). Hay viñetas vacías (líneas 14, 17, 194, 198 y 238) y residuos de exportación ("None"). | Agregar URL, licencia CC BY 4.0, fecha de descarga, registros por archivo, procesos tras la deduplicación y tamaño descomprimido. Limpiar el formato. |
| G-03 | 🟠 | **Hay compromisos del documento de selección que no se implementaron.** Faltan: distribución de **modalidades** en el EDA, "**tipo de entidad**" en las transacciones, "**identificador normalizado del proveedor** / variaciones de nombres", CMS sobre **entidades, proveedores y modalidades**, y los filtros web de **proveedor** y **rango de monto**. | Implementarlos o justificar por escrito por qué se descartaron. El profesor puede contrastar ambos documentos. |
| G-04 | 🟡 | El volumen se justifica solo con el tamaño comprimido. No hay ninguna medición que respalde "se necesita Spark". | Reportar registros, tamaño en memoria y tiempos de cada etapa en Spark frente a pandas sobre una muestra (ver I-06). |

### 3.1 Parte I: KDD, EDA y Spark

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| I-01 | 🔴 | **No se explica el particionamiento ni cómo afecta al plan.** Se escribe con `partitionBy('year','month')` y `shuffle.partitions=8` ([config.py:16](../src/config.py#L16)), pero ningún texto ni experimento lo analiza. Los planes se vuelcan como JSON crudo dentro de un `<pre>`. | Correr una consulta filtrada por año y mostrar el *partition pruning* (`PartitionFilters` en `explain()`). Comparar `shuffle.partitions` de 8, 64 y 200, y `repartition(key)` frente a ninguno: tiempo, número de tareas y cantidad de *Exchange*. Explicar el plan en lenguaje natural en la web. |
| I-02 | 🔴 | **El EDA no está todo en Spark.** El enunciado dice "obligatorio en toda esta parte". El HHI, la participación del top 10 y `top_suppliers` se calculan en **pandas** después de `toPandas()` ([part1/run.py:22](../src/part1/run.py#L22), [part1/run.py:35-37](../src/part1/run.py#L35-L37)). La API también recalcula HHI y rankings en pandas. | Calcular HHI, top-N y participaciones con funciones de ventana de Spark (`Window`, `percent_rank`, `sum over`). La API solo debe filtrar artefactos ya agregados. |
| I-03 | 🟠 | **Falta la distribución por modalidad (`method`).** No está en el cubo ([part1/run.py:14](../src/part1/run.py#L14)) ni en la web, aunque el documento de selección la promete. | Agregar `method` al cubo, a un gráfico y a los filtros. |
| I-04 | 🟠 | **La distribución de montos es pobre.** Solo hay una tabla de 4 bandas. Falta un histograma en escala log, percentiles y un boxplot por categoría o año, y la tabla está escondida en "detalle técnico". | Generar histogramas log10 con `percentile_approx` en Spark y mostrarlos en un gráfico principal con su interpretación. |
| I-05 | 🟠 | **Las interpretaciones del EDA son plantillas.** Por ejemplo: "Las diez entidades principales concentran X%… no demuestra irregularidad". No se dice qué entidades son, por qué ni si eso es esperable en el Perú (gobiernos regionales nuevos en 2023, picos de diciembre por cierre presupuestal, peso de Lima, etc.). | Escribir a mano 1 o 2 párrafos por hallazgo, con cifras reales y contexto peruano. |
| I-06 | 🟠 | **La comparación RDD vs. DataFrame se queda en tiempos.** El enunciado pide comparar **claridad y expresividad**. Las dos operaciones (`category_amount`, `buyer_count`) son triviales. La columna `shuffle` es un string fijo. | Mostrar el código de ambos enfoques lado a lado en la web. Explicar por qué el RDD es más lento (serialización Python, sin Catalyst ni Tungsten). Elegir al menos una operación no trivial ligada a las preguntas, por ejemplo un HHI por entidad o una agregación con join entre proveedores y procesos. |
| I-07 | 🟡 | Falta una **sección KDD explícita** en la web (selección → preprocesamiento → transformación → minería → interpretación). Hoy está dispersa entre el README y el detalle técnico. | Crear un módulo "Metodología KDD" con un diagrama del pipeline y el esquema final de las tablas. |
| I-08 | 🟡 | Las inconsistencias de nombres de proveedor no se tratan: un mismo RUC aparece con nombres distintos y no se unifican consorcios. | Normalizar `supplier_name` por `supplier_id` (moda o el más reciente) y documentar cuántos casos hubo. |
| I-09 | ⚪ | `process_features` es una copia idéntica de `processes` ([build_processes.py:111](../src/processing/build_processes.py#L111)). Duplica disco y tiempo. | Borrarla o hacer que de verdad contenga las *features* (tokens, shingles, canasta). |

### 3.2 Parte II: similitud, MinHash y LSH

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| II-01 | 🔴 | **No se compara el Jaccard estimado por MinHash contra el exacto.** El enunciado dice: "Calcular la similitud de Jaccard exacta sobre un subconjunto **y su aproximación con MinHash, comparando ambos resultados**". El código solo usa las firmas para crear bandas LSH. Nunca calcula `Ĵ = (#componentes iguales)/k`. | Para k ∈ {16, 32, 64, 128, 256}, calcular Ĵ en todos los pares de la muestra y reportar MAE, RMSE, un gráfico de dispersión Ĵ vs. J y el error frente a k (teórico ≈ 1/√k). Es además el primer parámetro interactivo que sugiere el enunciado. |
| II-02 | 🔴 | **LSH solo corre sobre 600 procesos.** El objetivo es "encontrar similares **sin comparar todos los pares**, usando Spark". Con 600 documentos la fuerza bruta es instantánea, así que no se demuestra el valor de LSH ni de Spark. | Correr MinHash+LSH sobre **todo** el corpus (unos 198 000) con Spark (`MinHashLSH` de MLlib o la versión propia en RDD). Evaluar precisión y recall en la muestra donde existe verdad exacta. Reportar cuántos pares se evitaron comparar (por ejemplo N(N−1)/2 ≈ 2·10¹⁰ frente a los candidatos). |
| II-03 | 🟠 | **Una muestra aleatoria de 600 casi no tiene pares similares** en umbrales altos (el README lo admite). Con tan pocos positivos, precisión y recall son ruido. | Muestrear por bloques (misma entidad, categoría o año) o enriquecer con pares de alta similitud para tener suficientes positivos. Reportar `truth_pairs` en cada umbral. |
| II-04 | 🟠 | **El caso interpretado dice literalmente "Revisión interpretativa asistida por IA… requiere validación humana"** ([case_notes.json:2](../src/part2/case_notes.json#L2)). Está escrito a mano en el código y se descarta si el par no cae en la muestra. Además está oculto en el detalle técnico. | Revisar a mano al menos 2 casos con el expediente en SEACE (enlace al `ocid`), redactarlos el equipo y mostrarlos en la vista principal. Quitar la etiqueta de IA solo después de validarlos de verdad. |
| II-05 | 🟠 | Falta la **curva S teórica** `P = 1 − (1 − s^r)^b` y el umbral efectivo `(1/b)^(1/r)`. El selector de configuración es un string (`h32_b16_r2_t0.5`), no sliders de bandas y filas. | Agregar sliders b/r que recalculen la curva S **en vivo** (es analítico y barato) y superpongan los puntos observados. |
| II-06 | 🟡 | La lista de stopwords tiene 18 palabras ([normalize_text.py:3](../src/processing/normalize_text.py#L3)). Términos como "servicio", "adquisición", "contratación" o "ítem" dominan los shingles e inflan la similitud. Eliminar tokens de un carácter vuelve idénticos textos como "-1" y "-2" (el propio caso lo reconoce). | Agregar stopwords de dominio derivadas de la frecuencia documental (por ejemplo df > 30 %). Justificar el ancho del shingle (palabras frente a caracteres, k=2 frente a k=3) con un experimento. |
| II-07 | 🟡 | El Jaccard exacto se calcula en Python en el driver y LSH en Spark, así que comparar tiempos mezcla motores distintos. La tabla también excluye el tiempo de las firmas. | Medir ambos en el mismo motor o explicar la asimetría, e incluir el costo de las firmas. |
| II-08 | ⚪ | No se considera SimHash ni se explica por qué no. | Una línea de justificación basta. |

### 3.3 Parte III: ANN

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| III-01 | 🔴 | **Falta la explicación conceptual de IVF y HNSW.** El enunciado la exige ("Explicar conceptualmente IVF y HNSW") y la rúbrica la evalúa ("Comprensión de IVF y HNSW"). La web solo dice "IVF: Bono opcional". | Agregar una sección con diagrama: grafo jerárquico de HNSW (capas, M, efConstruction, efSearch) e IVF (k-means, `nlist`, `nprobe`). Indicar qué parámetro controla cada compromiso. |
| III-02 | 🟠 | **El corpus de 5 000 es demasiado pequeño.** La búsqueda exacta sobre 5 000×4 096 tarda alrededor de 1 ms y la propia web avisa que el *speedup* puede ser menor que 1. Esto debilita la conclusión. | Medir el *speedup* con N ∈ {5k, 20k, 50k, 100k, 198k} usando TF-IDF disperso o SVD a 256 dimensiones, y graficar dónde se cruzan ANN y exacto. |
| III-03 | 🟠 | **IVF no está implementado.** Es un bono, pero con FAISS (`IndexIVFFlat`) son unas 20 líneas y da una comparación de dos métodos. | Agregar IVF con una rejilla de `nlist` y `nprobe`. |
| III-04 | 🟡 | Solo hay 40 consultas, sin intervalo de confianza. | Usar al menos 200 consultas y reportar recall medio ± IC. |
| III-05 | 🟡 | No se reutiliza la representación de la Parte II (shingles) como recomienda el enunciado, ni se justifica por qué TF-IDF es mejor para ANN. | Justificarlo en un párrafo, o comparar recall entre TF-IDF y binario sobre shingles. |
| III-06 | ⚪ | No se reporta la memoria de la búsqueda exacta (matriz densa) frente al índice. | Agregar `x.nbytes` a la tabla. |

### 3.4 Parte IV: minería de flujos

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| IV-01 | 🔴 | **Count-Min Sketch cuenta `category`, que tiene unos 3 o 4 valores** (bienes, servicios, obras) ([part4/run.py:28](../src/part4/run.py#L28)). El error sale cero y el ejercicio no prueba nada. La web misma admite que "un diccionario exacto puede ser más pequeño". El documento de selección prometía entidades, proveedores y modalidades. | Aplicar CMS a **proveedores (`supplier_id`)**, a **pares entidad–proveedor** o a **códigos UNSPSC de ítems**, que tienen decenas de miles de valores. Reportar error en heavy hitters frente a la cola, y la cota ε·N con probabilidad 1−δ. |
| IV-02 | 🟠 | **Bloom sobre `buyer_id`** ([part4/run.py:21](../src/part4/run.py#L21)): solo hay unos miles de entidades, así que cualquier filtro de 8 KB alcanza. La tarea no está ligada a ninguna pregunta. | Usar proveedores o pares entidad–proveedor (por ejemplo, "¿este proveedor ya ganó antes con esta entidad?"), que se conecta con la concentración. |
| IV-03 | 🟠 | **No hay parámetros en vivo.** El enunciado sugiere Bloom (m, k → FPR) y CMS (w, d → error) como recomputación analítica en vivo. Hoy Bloom y CMS solo muestran una rejilla estática. | Agregar sliders m/k que dibujen la curva teórica de FPR y superpongan la observada, y sliders w/d con ε y δ. |
| IV-04 | 🟡 | DGIM solo usa ventanas de **eventos**. El documento de selección habla de "ventanas temporales recientes". | Agregar una variante con ventana en días, o justificar por qué se usan eventos. Ligarla a una pregunta (por ejemplo "procesos de alto monto en los últimos N procesos de una región"). |
| IV-05 | 🟡 | El análisis precisión/memoria/tiempo existe en tablas, pero no hay texto que concluya, para cada estructura, qué configuración elegiría el equipo y por qué. | Escribir una conclusión por estructura. |
| IV-06 | ⚪ | `n%max(100,500)` es una forma confusa de escribir `n%500`. La variable `window` en [Streaming.jsx](../frontend/src/pages/Streaming.jsx) oculta el `window` global del navegador. | Limpiar. |

### 3.5 Parte V: reglas de asociación

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| V-01 | 🔴 | **Las 8 "interpretaciones" son una plantilla** ([part5/run.py:50](../src/part5/run.py#L50)): "Entre procesos con X, Y% también tienen Z… Es una asociación descriptiva, no causal". El enunciado pide "interpretar **en el contexto real del dataset, más allá de la métrica**". | Redactar a mano cada una de las 8 reglas: qué significa en la contratación peruana (normativa de la Ley 30225/32069, umbrales de modalidades, comportamiento regional) y si es esperable o sorprendente. |
| V-02 | 🟠 | **Las reglas no apuntan a ninguna pregunta.** Con 6 atributos de baja cardinalidad y soporte del 2 %, salen reglas obvias (por ejemplo, modalidad → banda de monto, que es casi una definición legal). | Minar reglas **con consecuente fijo**, por ejemplo `POSTORES=uno` (baja competencia), y reportar qué combinaciones elevan el lift de tener un solo postor. Separar las reglas triviales o normativas de las informativas. |
| V-03 | 🟠 | Las reglas que contienen "sin dato" se descartan en silencio ([part5/run.py:36](../src/part5/run.py#L36)). Si `POSTORES=sin dato` es frecuente, eso **es un hallazgo de calidad de datos**. | Reportar la proporción de "sin dato" por atributo y discutirla. |
| V-04 | 🟡 | La canasta no se justifica a fondo: por qué esos 6 atributos, por qué esas bandas de monto y por qué `ANIO` entra para luego filtrarse. Falta "tipo de entidad" (prometido). | Justificar o ajustar. Considerar el nivel de gobierno (nacional, regional o local) derivado del nombre o el RUC de la entidad. |
| V-05 | ⚪ | [test_api.py:32](../scripts/test_api.py#L32) exige `found == 8`. Si los datos cambian, el test falla aunque el código esté bien. | Usar `>= 8` o validar el estado. |

### 3.6 Parte VI y reporte web

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| VI-01 | 🔴 | **No hay enlace publicado ni video** (requisito mínimo). La web depende de FastAPI más artefactos que no están en el repositorio, así que nadie puede verla sin correr el pipeline de unos 3 GB. | (a) Grabar un video de 5–8 min. (b) Publicar: exportar los artefactos como JSON estático y servir el frontend desde GitHub Pages o Vercel sin API, o desplegar API y web en Render o Railway. |
| VI-02 | 🔴 | **Las preguntas de discusión del enunciado no se responden con profundidad.** "10× o 100×" es una sola oración escondida ([Discussion.jsx:32](../frontend/src/pages/Discussion.jsx#L32)). "¿Qué tareas se beneficiaron de Spark y cuáles no?" no se responde con mediciones. "Implicancias de usar datos públicos peruanos (transparencia, rendición de cuentas, uso indebido)" tiene 4 viñetas genéricas. | Escribir una sección por cada una de las 5 preguntas del enunciado, con evidencia propia: tiempos de etapas Spark frente a pandas, una proyección a 10× y 100× con costo estimado, y una discusión ética (personas naturales como proveedores, que un RUC personal es casi el DNI, riesgo de estigmatizar entidades, sesgos por calidad de datos y omisiones de registro). |
| VI-03 | 🟠 | **La anatomía de cada hallazgo está incompleta.** El enunciado exige "pregunta → **arquitectura del pipeline** → visualización → controles → interpretación". Ningún módulo muestra el pipeline que generó el resultado. | Agregar a cada página un mini diagrama (fuente → transformación → algoritmo → artefacto), indicando si corre en Spark o en local. |
| VI-04 | 🟠 | **La metodología se muestra como `JSON.stringify` dentro de `<pre>`** (Similarity, Streaming, planes Spark). Eso no es "texto de metodología embebido". | Reemplazarlo por prosa redactada. |
| VI-05 | 🟠 | **La cita de la fuente está incompleta en la web**: solo dice "Fuente: OECE · OCDS" ([App.jsx:48](../frontend/src/App.jsx#L48)), sin enlace, licencia ni fecha. | Agregar un pie con la URL `data.open-contracting.org/es/publication/135`, la licencia CC BY 4.0 y la fecha de descarga. |
| VI-06 | 🟠 | **El contexto peruano casi no aparece.** No se mencionan el ciclo presupuestal, los gobiernos subnacionales, las modalidades de la normativa de contrataciones, OECE/SEACE, ni hallazgos de Contraloría o prensa que contrasten los resultados. | Incluir al menos un párrafo de contexto por hallazgo. |
| VI-07 | 🟡 | Hay una cifra fija en el código: "198 mil procesos" ([Overview.jsx:14](../frontend/src/pages/Overview.jsx#L14)). Los años también están fijos en [Association.jsx:17](../frontend/src/pages/Association.jsx#L17) y [Quality.jsx:10](../frontend/src/pages/Quality.jsx#L10). | Leerlos de la API. |
| VI-08 | 🟡 | El gráfico "Procesos y montos convocados por año" solo dibuja `processes` ([Overview.jsx:28](../frontend/src/pages/Overview.jsx#L28)). | Agregar el monto (eje secundario o gráfico aparte) o corregir el subtítulo. |
| VI-09 | 🟡 | En Concentración, el ranking de proveedores siempre usa el monto atribuido, aunque la métrica elegida sea "número de procesos". | Respetar la métrica elegida o rotularlo. |

### 3.7 Código y reproducibilidad

| ID | Sev. | Observación | Qué hacer |
|---|---|---|---|
| C-01 | 🔴 | **Ningún módulo indica si corre en Spark o en local, ni por qué** (requisito explícito de los entregables). | Agregar un docstring de cabecera en cada `src/partN/run.py` con *Motor: Spark/local · Motivo · Entradas · Artefactos*. |
| C-02 | 🟠 | **El código es denso y casi no tiene comentarios**: muchas sentencias por línea separadas con `;` y comprensiones anidadas de más de 200 caracteres (por ejemplo [part2/run.py](../src/part2/run.py), [part4/run.py](../src/part4/run.py) y [run_all.py](../src/run_all.py)). Afecta el punto de "organización, comentarios" y complica la **sustentación oral**: el profesor puede pedir que se explique cualquier línea. | Pasar `black`/`ruff format` y dividir en funciones con nombre (`compute_exact_jaccard`, `evaluate_lsh_config`…). Comentar el *porqué* de las decisiones no obvias. |
| C-03 | 🟠 | **`download.py` no descarga nada** ([download.py](../src/ingestion/download.py)): solo vuelve a validar. El README no da las URLs exactas de cada archivo anual. Además, el repositorio entero se monta como `/data/raw` ([docker-compose.yml:7](../docker-compose.yml#L7)), así que hay que copiar unos 3 GB a la raíz. | Implementar la descarga real (`requests` y `.jsonl.gz` por año) con verificación de hash, y usar una carpeta `data/raw/` dedicada. |
| C-04 | 🟠 | [PLAN_EJECUCION_OECE_JSON_DOCKER.md](../PLAN_EJECUCION_OECE_JSON_DOCKER.md) describe archivos que no existen (`inspect_schema.py`, `build_items.py`, `evaluate_lsh.py`, `exact_search.py`, etc.) y habla de IVF. | Actualizarlo a la estructura real o moverlo a `docs/` como histórico. |
| C-05 | 🟡 | **No hay artefactos de muestra en el repositorio**, así que un evaluador no puede ver resultados sin una corrida completa. | Versionar un `artifacts_sample/` ligero (menos de 20 MB) o publicarlo como release, y que la web pueda cargarlo. |
| C-06 | 🟡 | `vitest` está declarado, pero no hay tests de frontend. `test_frontend.py` solo comprueba que el HTML carga. | Agregar al menos un test de render por página o quitar la dependencia. |
| C-07 | ⚪ | El `run_manifest` intenta `git rev-parse` dentro del contenedor, pero `.git` no se copia, así que siempre queda `uncommitted`. | Pasar `CODE_VERSION` como build-arg. |
| C-08 | ⚪ | `apply_historical_rates.daily/annual/period` usa atributos de función como estado global ([build_processes.py](../src/processing/build_processes.py)). | Pasarlos como parámetros. |
| C-09 | ⚪ | El historial de git muestra un solo autor. Si se pide sustentación oral, los tres integrantes deben poder explicar todo. | Repartir la sustentación por partes y practicarla. |

---

## 4. Errores de código detectados

| ID | Archivo | Problema | Corrección |
|---|---|---|---|
| B-01 | [Ann.jsx:11](../frontend/src/pages/Ann.jsx#L11) | Si `/api/ann` falla, `query` nunca se asigna y la condición `!query` muestra **Loading para siempre**. El `ErrorState` de la línea siguiente nunca se ve. | Revisar `initial.error` **antes** de la condición de carga. |
| B-02 | [Similarity.jsx:13](../frontend/src/pages/Similarity.jsx#L13) | Mismo patrón: si `base` falla, `active` queda vacío y la página se queda cargando indefinidamente. | Igual que B-01. |
| B-03 | [Overview.jsx:14](../frontend/src/pages/Overview.jsx#L14) | "198 mil procesos" está fijo en el código y no coincide si cambian los años o la deduplicación. | Usar `q.selected_processes`. |
| B-04 | [test_api.py:26](../scripts/test_api.py#L26), [test_api.py:32](../scripts/test_api.py#L32) | Los tests de la API dependen de valores exactos (36 configuraciones, 8 reglas). | Validar invariantes, no cantidades exactas. |

---

## 5. Propuesta: preguntas de interés que integren todas las técnicas

El mayor cambio de nota viene de **reorganizar la narrativa** alrededor de 3 preguntas de dominio y usar cada técnica como herramienta para responderlas, en vez de presentar 5 "experimentos".

| # | Pregunta de interés (propuesta) | Relevancia práctica | Técnicas que la responden |
|---|---|---|---|
| Q1 | **¿Qué tan concentrada está la adjudicación en pocos proveedores dentro de cada entidad y departamento, y cambió entre 2023 y 2025?** | Detectar mercados con poca competencia y dependencia de proveedores. Es una señal para la supervisión de la OECE y la Contraloría. | EDA y Spark (HHI por entidad y departamento), Bloom (¿el proveedor ya ganó con la entidad?), CMS (frecuencia de proveedores en el flujo) |
| Q2 | **¿Qué características del proceso (modalidad, monto, departamento, categoría, nivel de gobierno) se asocian a tener un solo postor?** | La baja competencia encarece las compras. Identificar dónde ocurre permite focalizar políticas. | EDA (distribución de postores), FP-Growth con consecuente `POSTORES=uno`, DGIM (procesos de postor único o alto monto en ventanas recientes) |
| Q3 | **¿Existen requerimientos casi idénticos convocados repetidamente por una misma entidad en periodos cortos, o por muchas entidades a la vez?** | Lo primero puede indicar fraccionamiento o reconvocatorias. Lo segundo es una oportunidad de compras corporativas o de catálogo (Perú Compras). | Shingling, MinHash y LSH sobre todo el corpus; ANN (HNSW/IVF) para buscar "procesos parecidos a este"; DGIM en ventana temporal |

Cada página web debería abrir con una de estas preguntas y cerrar con una respuesta redactada por el equipo, con cifras y sus límites.

---

## 6. Plan de trabajo priorizado

### Prioridad 1: requisitos explícitos incumplidos (hacer primero)
- [ ] **G-01** Redefinir 2 o 3 preguntas de interés (§5) y reorganizar la web alrededor de ellas.
- [ ] **II-01** Comparar el Jaccard estimado por MinHash con el exacto (error vs. k), con slider interactivo.
- [ ] **II-02** Correr LSH sobre el corpus completo en Spark y reportar los pares evitados.
- [ ] **III-01** Agregar la explicación conceptual de HNSW e IVF, con diagrama.
- [ ] **IV-01** Mover CMS a un dominio de alta cardinalidad (proveedores o UNSPSC).
- [ ] **V-01** Redactar a mano la interpretación de las 8 reglas en contexto.
- [ ] **I-01** Hacer el experimento de particionamiento y explicar `explain()`.
- [ ] **I-02** Pasar HHI, top-N y participaciones a Spark.
- [ ] **VI-01** Grabar el video y/o publicar la web.
- [ ] **VI-02** Escribir la discusión crítica completa (5 preguntas del enunciado, con evidencia).
- [ ] **C-01** Agregar un docstring Spark/local con su motivo en cada módulo.

### Prioridad 2: calidad del análisis y la comunicación
- [ ] **I-03, I-04** Agregar la distribución por modalidad y un histograma log de montos.
- [ ] **I-05, VI-06** Escribir interpretaciones propias con contexto peruano en cada hallazgo.
- [ ] **I-06** Comparar RDD y DataFrame en claridad, con código lado a lado.
- [ ] **II-03, II-04, II-05** Muestreo por bloques, casos validados por humanos y curva S en vivo.
- [ ] **III-02, III-03** Medir el *speedup* según el tamaño del corpus y agregar IVF (bono).
- [ ] **IV-02, IV-03** Bloom sobre proveedores y sliders analíticos para Bloom y CMS.
- [ ] **V-02, V-03** Reglas con consecuente `POSTORES=uno` y análisis de "sin dato".
- [ ] **VI-03, VI-04, VI-05** Diagrama del pipeline por hallazgo, prosa en lugar de JSON y cita completa de la fuente.
- [ ] **G-02, G-03** Completar el documento de selección y cerrar los compromisos pendientes.

### Prioridad 3: código y reproducibilidad
- [ ] **B-01, B-02, B-03, B-04** Corregir los errores.
- [ ] **C-02** Formatear y refactorizar el código denso, con comentarios del *porqué*.
- [ ] **C-03** Descarga real y carpeta `data/raw/`.
- [ ] **C-04** Actualizar o archivar el plan.
- [ ] **C-05** Versionar artefactos de muestra.
- [ ] **I-08, I-09, IV-06, C-06, C-07, C-08** Limpieza menor.

---

## 7. Nota esperada si se atienden las mejoras

| Escenario | Nota estimada |
|---|---:|
| Estado actual | ≈ 11 / 20 |
| Solo Prioridad 1 | ≈ 15 / 20 |
| Prioridades 1 y 2 | ≈ 17–18 / 20 |
| Prioridades 1, 2 y 3, más IVF (bono) y una sustentación sólida | ≈ 18–20 / 20 |

> La base técnica permite llegar a una nota alta. Lo que falta es sobre todo **análisis escrito por el equipo, preguntas de dominio y tres requisitos explícitos** (MinHash vs. exacto, explicación de IVF/HNSW y particionamiento), no más infraestructura.
