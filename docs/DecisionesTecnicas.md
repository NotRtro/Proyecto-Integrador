# Decisiones técnicas: volumen de datos, muestras y tiempo de ejecución

Este documento explica qué datos usa cada tarea del proyecto, por qué, cuánto cuesta y qué se perdería con un atajo. Las cifras salen de las corridas reales (`artifacts/metadata/data_decisions.json`, que el pipeline regenera en cada corrida, y los manifiestos de ejecución). La web muestra la misma tabla en *Datos y KDD → Decisiones de volumen*.

## El principio

**Cada tarea usa el volumen que su pregunta necesita.** Se muestrea donde la muestra conserva la conclusión y ahorra tiempo de verdad; no se muestrea donde la muestra cambia la respuesta. Así lo pide el enunciado: *"trabajar con un subconjunto representativo es válido si el volumen total no es manejable, siempre que se justifique"*.

Eso lleva a cuatro decisiones:

1. **Q1, Q2 y Q3 se responden con todos los datos.** Son las tareas baratas y las que una muestra rompe.
2. **Las referencias exactas y las comparaciones de algoritmos usan muestras justificadas** (Jaccard exacto, Apriori y, en el perfil rápido, ANN).
3. **Las mediciones de una vez no se repiten en cada corrida** (benchmark RDD/DataFrame, particionamiento, Spark vs. pandas, escalamiento de ANN).
4. **Lo que no cambió no se recalcula**: la validación y el parseo del JSON se reutilizan si los archivos y el código no cambiaron.

## Qué datos usa cada tarea

| Tarea | Responde | Datos | Por qué |
|---|---|---|---|
| Parseo, limpieza y tablas | todas | todos los registros | Es la base y la tarea que justifica Spark por volumen (JSON anidado de ~3 GB) |
| HHI por entidad y departamento | Q1 | todos los procesos | El HHI de una entidad necesita todas sus adjudicaciones |
| Tasa de postor único, EDA | Q2 | todos los procesos | Los grupos de interés (modalidad × departamento) son pequeños |
| MinHash + LSH | Q3 | todos los procesos con texto | Buscar repeticiones exige tener ambos procesos de cada par |
| Bloom, Count-Min, DGIM | Q1, Q2 | todos los eventos, en orden | El estado de un flujo es su historia completa |
| FP-Growth | Q2 | todas las canastas | Es barato y las reglas dirigidas necesitan grupos pequeños |
| Jaccard exacto (referencia) | Parte II | muestra por bloques de 1 500 procesos | Es cuadrático; solo mide el error de MinHash y LSH |
| Apriori (referencia) | Parte V | muestra de 10 000 canastas | Solo valida que FP-Growth encuentra lo mismo |
| Evaluación de ANN | Parte III | todo (perfil completo) o 50 mil (perfil rápido) | Compara algoritmos; no responde una pregunta sobre el Estado |

## Por qué Q1, Q2 y Q3 no se muestrean: la evidencia

Se repitió cada análisis sobre muestras para ver qué se pierde (`eda/sampling_effect.csv` y `data_decisions.json`).

**Q1 (concentración).** HHI de proveedores por entidad (mínimo 5 adjudicaciones), muestra frente a todos los datos, año 2025:

| Muestra | Entidades evaluables | HHI mediano | Con HHI > 2 500 | Mismo proveedor principal |
|---|---|---|---|---|
| 100 % | 1 765 | 2 016 | 37,4 % | — |
| 30 % | 767 (43 %) | 2 401 | 47,5 % | 39 % |
| 10 % | 241 (14 %) | 2 822 | 58,1 % | 17 % |

Con el 10 % desaparece el 86 % de las entidades. Con pocas adjudicaciones por entidad, el HHI se infla: dos proveedores ya parecen un mercado concentrado. La muestra no solo pierde precisión: **exagera la dependencia**, que es justo lo que la pregunta mide.

**Q3 (requerimientos repetidos).** Un par sobrevive solo si sus dos procesos entran en la muestra, así que con una muestra del p % sobrevive cerca del p² de los pares:

| Muestra | Pares casi idénticos que sobreviven | Esperado (p²) | Entidades que repiten en ≤ 30 días detectadas |
|---|---|---|---|
| 100 % | 130 168 | — | 2 423 |
| 30 % | 13 112 (10,1 %) | 9 % | 1 002 (41 %) |
| 10 % | 1 456 (1,1 %) | 1 % | 203 (8 %) |

Con el 10 % se pierde ~99 % de los pares y la mayoría de las entidades que repiten requerimientos. La pregunta deja de poder responderse.

**Costo de no muestrear.** Estas tareas tardan poco con todos los datos: concentración 26 s, EDA y tasas de postor único 47 s, LSH y clasificación de la Q3 67 s, flujos 98 s y FP-Growth 11 s (corrida completa). Juntas suman unos 4 minutos. El tiempo de la corrida no se va en responder las preguntas.

## Dónde sí se muestrea, y su matiz

- **Jaccard exacto:** con 1 500 procesos son 1,1 millones de pares; con el corpus completo serían 19 262 millones. La muestra es por bloques de entidades completas, para que haya pares similares que medir (538 con Jaccard ≥ 0,7).
- **Apriori:** recorre todas las canastas en cada nivel. Sobre 10 000 canastas basta para comprobar que FP-Growth encuentra exactamente los mismos conjuntos.
- **ANN en el perfil rápido (50 mil procesos):** el recall@10 de HNSW es 99,2 % con 50 mil procesos frente a 96,9 % con los 196 mil, pero la aceleración frente a la búsqueda exacta es 32× frente a 164×, porque la búsqueda exacta crece con N y HNSW casi no. La conclusión se mantiene (HNSW es mucho más rápido con recall alto), pero **la magnitud de la aceleración crece con el tamaño del corpus**. Por eso las cifras del informe salen del perfil completo y el rápido se usa para trabajar.

## Mediciones de una vez

El benchmark RDD/DataFrame, el experimento de particionamiento, Spark vs. pandas, el escalamiento de ANN y el efecto de muestrear la Q1 comparan motores y algoritmos; no cambian si cambian los datos de un mes. Se controlan con `--experiments`:

| Modo | Qué hace | Por defecto en |
|---|---|---|
| `si` | Las mide de nuevo | perfil completo |
| `auto` | Las mide solo si faltan sus resultados | perfil rápido |
| `no` | No las mide; la web muestra la última medición | — |

`artifacts/metadata/experiments.json` registra, para cada una, la fecha, el perfil, la versión del código y la duración de la última medición. Si se cambia el código de un experimento, hay que volver a medirlo con `--experiments si`.

## Cachés

| Qué | Se reutiliza si | Ahorro |
|---|---|---|
| Validación de archivos (SHA-256 y conteos de ~3 GB) | nombre, tamaño y fecha de los archivos coinciden con el manifiesto | ~1,8 min (110 s → 0 s) |
| Staging (parseo del JSON a Parquet) | no cambiaron los archivos **ni el código que los parsea** | ~2 min |
| Procesamiento completo (deduplicación, monedas, seis tablas, calidad) | además no cambiaron el código del procesamiento, los años ni el umbral de stopwords | ~5,5 min (336 s → 13 s) |

`--refresh` ignora todas las cachés. Las huellas incluyen el código: si alguien modifica `model.py`, `build_processes.py` o `text_features.py`, la caché se invalida sola y nunca se reutilizan tablas hechas con código viejo.

## Modos de ejecución y tiempos medidos

Medidos en una laptop con 8 núcleos y 8 GB para Docker, con el clúster de dos workers del proyecto.

| Modo | Comando | Tiempo | Para qué |
|---|---|---|---|
| Muestra de desarrollo (3 %), en frío y con todas las mediciones | `make dev` | 8,7 min | Probar de punta a punta un cambio de código; cifras no válidas |
| Rápido, primera vez (sin caché) | `make rapido` | 15,7 min | Primera corrida tras descargar datos nuevos |
| **Rápido con caché** (datos y código del procesamiento sin cambios) | `make rapido` | **8,1 min** | Trabajo diario sobre **todos** los datos |
| Completo con mediciones | `make run` | 25,1 min (validación y parseo en caché) · 26,7 min en frío | Corrida del informe y de la web publicada |

Antes de estas decisiones, cada corrida tardaba unos 27 minutos. **Con caché, una corrida rápida sobre todos los datos (8,1 min) cuesta lo mismo que la muestra del 3 % en frío (8,7 min).** Achicar los datos ya no ahorra tiempo; lo que ahorra es no repetir trabajo. Por eso la muestra queda solo para probar código, no para producir resultados.

Cómo usarlos:

```bash
make rapido                    # trabajo diario sobre todos los datos
make run                       # perfil completo: la corrida del informe
make dev-sample && make dev    # probar cambios de código en minutos (cifras no válidas)
# sin make:
docker compose run --rm pipeline python -m src.run_all --profile rapido
docker compose run --rm pipeline python -m src.run_all --profile completo --experiments si --refresh
```

## La web no ejecuta nada

El reporte nunca corre Spark: lee resultados ya calculados y responde al instante. La versión publicada ni siquiera necesita el pipeline (JSON estáticos). El tiempo de una corrida no afecta la demostración ni el video; solo importa cuando se cambia código o datos.

## Riesgos de estos atajos y cómo se controlan

| Riesgo | Control |
|---|---|
| Mostrar una medición vieja como si fuera actual | `experiments.json` guarda fecha y versión del código; la web muestra el estado de cada medición |
| Reutilizar un parseo hecho con código anterior | La huella del staging incluye el código del parseo |
| Presentar cifras de ANN del perfil rápido como definitivas | `ann/methodology.json` y la tabla de decisiones dicen con qué perfil y tamaño se evaluó; el informe usa el perfil completo |
| Usar la muestra de desarrollo en el informe | El README y el script de muestra lo advierten; esas cifras viven en `data/dev/`, separadas |
