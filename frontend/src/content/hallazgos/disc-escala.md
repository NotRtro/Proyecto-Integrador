**Con 10 veces más datos (unos 2 millones de procesos, ~30 GB de JSON), el diseño actual aguanta con ajustes.**

- **Ingesta:** el procesamiento crece casi en proporción al volumen, de 5,5 minutos a casi una hora en el clúster local de 4 núcleos. Bastaría con más workers: las tareas son independientes por partición. El cuello de botella sería gzip, que no se divide: habría que pedir o generar archivos más pequeños (por mes) para paralelizar la lectura desde el inicio.
- **Particiones:** las 8 particiones de shuffle quedarían cortas (tareas de cientos de MB). Habría que subirlas a 64–200 y mantener AQE para que las ajuste. Reparticionar por entidad pasaría a compensar en las consultas que agrupan varias veces por la misma clave.
- **Búsqueda exacta:** pasaría de 21 ms a unos 200 ms por consulta y la matriz de vectores, de 0,2 a 2 GB. Aún cabe en una máquina, pero un buscador interactivo ya necesitaría HNSW.
- **LSH:** los pares posibles pasarían de 19 mil millones a casi 2 billones. LSH seguiría funcionando, pero los buckets de plantillas crecerían diez veces y habría que subir r (más filas por banda) para mantener manejable el número de candidatos.

**Con 100 veces más datos (~20 millones de procesos, ~300 GB), cambian las decisiones.**

- **Ya no hay "versión local".** La tabla de procesos ocuparía unos 3 GB solo en sus columnas analíticas y el driver no podría recibir resultados intermedios grandes. Todo lo que hoy se trae al driver (la verdad exacta de la muestra, las estructuras de flujo, los índices ANN) tendría que distribuirse o resumirse antes.
- **ANN distribuido:** la búsqueda exacta tardaría unos 2 s por consulta y la matriz 20 GB. El índice HNSW no cabría cómodo en una máquina: habría que particionarlo (un índice por región o por categoría) o usar IVF con compresión (product quantization), que reduce la memoria a cambio de recall.
- **Flujos:** aquí los sketches dejan de ser didácticos. Un conjunto exacto de cientos de millones de pares entidad–proveedor no cabe en memoria; un Bloom Filter de pocos MB sí. Lo mismo para Count-Min sobre miles de millones de eventos.
- **Reglas:** FP-Growth sigue escalando con MLlib, pero con más atributos el árbol crece. Habría que subir el soporte mínimo o minar por segmentos.

**Qué no cambiaría:** las respuestas a Q1–Q3 seguirían calculándose con todos los datos. Son agregaciones lineales que Spark distribuye sin problema, y la evidencia de este proyecto muestra que una muestra cambia la respuesta. Lo que escala mal es lo cuadrático (pares y vecinos) y lo que vive en el driver. Eso es exactamente lo que las técnicas aproximadas del curso resuelven.

**Límite de la proyección.** Es lineal, a partir de las mediciones de este proyecto en una laptop. Los tiempos reales dependen del clúster, y los algoritmos con componentes superlineales, como los buckets grandes de LSH o la construcción de HNSW, crecerían algo más rápido.
