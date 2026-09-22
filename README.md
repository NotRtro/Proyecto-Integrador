# Minería de contrataciones OECE · 2023–2025

Proyecto basado en `PLAN_EJECUCION_OECE_JSON_DOCKER.md`. Procesa los tres archivos locales `peru_oece_bulk_2023.jsonl`, `peru_oece_bulk_2024.jsonl`, `peru_oece_bulk_2025.jsonl`, genera tablas Parquet, expone resultados mediante una API FastAPI y presenta cinco experimentos en una aplicación React.

## Ejecutar

Requisitos: Docker Desktop iniciado, Docker Compose v2, aproximadamente 10 GB de RAM asignados a Docker y espacio libre para imágenes y resultados. No se requiere Python ni Java en el anfitrión. Ejecutar desde esta carpeta:

```bash
make all
```

Equivale a:

```bash
docker compose build
docker compose up -d --wait spark-master spark-worker-1 spark-worker-2 api web
docker compose run --rm pipeline python -m src.run_all
```

Abrir **http://localhost:8501**. La documentación interactiva de la API está en **http://localhost:8000/docs**. Spark Master: http://localhost:8080; workers: http://localhost:8081 y http://localhost:8082.

Los archivos originales se montan en solo lectura. `artifacts/` y `data/processed/` son directorios persistentes del anfitrión; `docker compose down` no los elimina. No se duplican ni descargan los 3 GB ya proporcionados. También se admite `.jsonl.gz`. Evitar colocar copias de los mismos archivos en la raíz.

## Comandos

```bash
# Ejecuta pruebas de algoritmos, integración, API y frontend.
# Requiere artefactos generados para comprobar la API.
make test
# Validación, hashes y cobertura, sin Spark:
docker compose run --rm pipeline python -m src.ingestion.validate_files
# Compatibilidad con el paso de descarga del plan (registra entradas locales):
docker compose run --rm pipeline python -m src.ingestion.download
# Rehacer tablas:
docker compose run --rm pipeline python -m src.processing.build_processes
# Cada parte por separado, una vez construidas las tablas:
docker compose run --rm pipeline python -m src.part1.run
docker compose run --rm pipeline python -m src.part2.run
docker compose run --rm pipeline python -m src.part3.run
docker compose run --rm pipeline python -m src.part4.run
docker compose run --rm pipeline python -m src.part5.run
# Reanudar desde una etapa, conservando las anteriores:
docker compose run --rm pipeline python -m src.run_all --from-stage part2
# Estado y logs:
docker compose ps
docker compose logs -f web
make down
```

Una etapa independiente actualiza sus artefactos; `run_manifest.json` describe la última ejecución del orquestador, no esas invocaciones individuales. No ejecutar dos pipelines simultáneamente sobre los mismos directorios.

## Datos y criterios

- El archivo de 2025 contiene una línea con dos objetos completos concatenados. La ingesta los separa lógicamente, registra línea y cantidad en `metadata/repairs_*.json` y no modifica el original. Cualquier objeto incompleto o basura residual detiene la validación.
- La selección temporal usa `tender.datePublished` y, si falta o es inválida, `tender.tenderPeriod.startDate`. Nunca se toma el año del archivo ni `release.date` como año de convocatoria.
- Hay procesos históricos recompilados en estos archivos. Se conserva la versión más reciente de cada `ocid` por `release.date`, con desempate por nombre de archivo. No se reconstruye una fotografía histórica de las adjudicaciones.
- Se generan `processes`, `items`, `awards`, `suppliers`, `contracts`, `process_features`, particionados por año/mes. Los montos se agregan antes de desanidar proveedores.
- Se usa `amount_PEN` cuando existe; en su defecto, `amount` si `currency=PEN`. Para adjudicaciones extranjeras sin `amount_PEN`, se aplica la tasa implícita `tender.amount_PEN / tender.amount` cuando coincide la moneda. Los demás casos usan medianas históricas robustas, calculadas con conversiones OECE del mismo día, año o periodo; una tasa diaria fuera de 0.5–2 veces la mediana anual se descarta. La regla también cubre ítems y contratos. Se conservan monto, moneda, fecha, método y tasa; los faltantes nunca se convierten en cero.
- En adjudicaciones con múltiples proveedores, el monto se distribuye equitativamente para elaborar rankings sin duplicarlo. Esta atribución analítica no representa pagos individuales.
- Departamento corresponde al domicilio del comprador. Postores ausentes o menores que uno se clasifican como `sin dato`.
- Rangos de monto convocado: bajo <100 000; medio <1 000 000; alto <10 000 000; muy alto ≥10 000 000 PEN.
- No se publican datos de contacto personales presentes en los JSON originales.

## Técnicas y alcance

| Parte | Implementación | Referencia y alcance |
|---|---|---|
| I | Spark DataFrame, SQL interno y RDD | Dos operaciones equivalentes; calentamiento y tres ejecuciones; todos los procesos |
| II | MinHash de 32/64/128 hashes y bandas LSH distribuidas con Spark | Jaccard exhaustivo en muestra determinista de 600 procesos; umbrales .5/.6/.7/.8 |
| III | TF-IDF de unigramas/bigramas, coseno exacto y HNSW | Muestra de hasta 5000 textos, 40 consultas, recall@5/@10, latencias y tamaño |
| IV | Bloom, Count-Min Sketch y DGIM | Todos los eventos en orden temporal; comparación exacta en cada estructura |
| V | Apriori local y Spark FP-Growth | Misma muestra de hasta 10 000 para validar equivalencia; FP-Growth adicional sobre todos los procesos |

La semilla es 42. Las muestras se seleccionan por SHA256 de `ocid` más semilla. HNSW y BLAS de la referencia exacta utilizan un hilo para reproducibilidad. Los vectores exactos y aproximados son idénticos; no se aplica SVD. IVF es un bono opcional no incluido.

La precisión LSH mide candidatos **antes** de verificación exacta; la tabla contiene TP, FP y FN para cada configuración. Los tiempos de candidatos excluyen la construcción compartida de firmas. Una configuración sin pares verdaderos no demuestra recall útil. Los resultados de ANN no se extrapolan automáticamente al corpus completo y los empates pueden reducir recall de identificadores.

Bloom consulta antes de insertar y mide falsos positivos sobre primeras apariciones de entidades. Su tasa teórica final y la tasa acumulada observada representan ocupaciones distintas. CMS cuenta categorías; con tan pocas categorías el contador exacto puede ser más conveniente. DGIM usa ventanas de eventos y umbrales fijos de monto. La memoria es estimada (buffers o `sys.getsizeof`), no RSS.

Las reglas se minan a soportes 2 %, 5 % y 10 %. Se compara Apriori y FP-Growth sobre exactamente las mismas canastas. La web filtra reglas encontradas a soporte global mínimo 2 %; no vuelve a minar al cambiar filtros. Las métricas por año se recalculan sobre las canastas de cada año para esas reglas. Se seleccionan hasta ocho reglas no redundantes, con lift >1, confianza ≥50 % y soporte ≥2 %; nunca se inventan reglas si los criterios no se alcanzan.

## Configuración y recursos

Copiar `.env.example` a `.env` para ajustar memoria, semilla, años y tamaños de muestra. Compose fija los caminos internos `/data/raw`, `/data/processed`, `/artifacts` y el master. Las variables de caminos sirven también para ejecución local. Después de modificar código, ejecutar `docker compose build` y recrear servicios con `docker compose up -d`.

## API y aplicación React

La web sigue el criterio de doble lectura. Cada módulo presenta primero una conclusión ejecutiva con indicadores y gráficos. El botón **Ver detalle técnico** abre parámetros, benchmarks, referencias exactas, tablas y limitaciones metodológicas.

El navegador nunca lee los JSONL originales ni los CSV grandes. Nginx sirve la aplicación React en el puerto 8501 y reenvía `/api/*` al contenedor FastAPI. La API lee los artefactos precalculados montados en `/artifacts`, aplica filtros y devuelve únicamente datos agregados o páginas pequeñas.

Rutas principales:

| Ruta | Contenido |
|---|---|
| `/api/overview` | Resumen ejecutivo, estado y mejores configuraciones |
| `/api/quality` | Fuentes, cobertura, nulos y esquema |
| `/api/eda` | Agregaciones filtradas, rankings y concentración |
| `/api/similarity` | Rejilla LSH, pares y casos interpretados |
| `/api/ann` | Consultas, configuraciones y vecinos |
| `/api/streaming` | Bloom, CMS, DGIM y memoria |
| `/api/association` | Reglas filtradas, selección y benchmark |

Los endpoints que exponen filas incluyen límites para evitar respuestas sin control. FastAPI publica el contrato completo en `/docs` y `/openapi.json`.

El driver necesita memoria para la muestra ANN y reglas/candidatos; el Jaccard exacto crece cuadráticamente con `SAMPLE_SIZE`. No aumentar muestras sin medir. Los originales se leen por particiones Spark; los artefactos de la web contienen agregaciones y muestras, no documentos completos OCDS.

## Resultados y verificación

- `artifacts/metadata/source_manifest.json`: tamaño, SHA-256, fecha, registros y procedencia.
- `artifacts/metadata/run_manifest.json`: estado, tiempos, parámetros y versión.
- `artifacts/metadata/quality_summary.json`: descartes, duplicados, fechas, monedas y granos.
- `artifacts/{eda,similarity,ann,streaming,association}/`: resultados reales y parámetros.
- `artifacts/metadata/artifact_validation.json`: verificación final y pendientes académicos.
- `tests/`: invariantes de normalización, fechas, monedas, asignación de montos, Jaccard, MinHash, Bloom, CMS, DGIM y Apriori.

La definición académica del plan también requiere revisión humana de un caso y un enlace publicado o video. La aplicación local y el guion en `PRESENTACION.md` permiten preparar esa demostración. El pipeline no afirma haber realizado una revisión humana ni publicado una web.

Documentación técnica: [Spark standalone 3.5.6](https://spark.apache.org/docs/3.5.6/spark-standalone.html), [HNSWlib](https://github.com/nmslib/hnswlib), [FastAPI](https://fastapi.tiangolo.com/) y [React](https://react.dev/). Fuente declarada por el plan: [Registro OCDS OECE](https://data.open-contracting.org/es/publication/135).
