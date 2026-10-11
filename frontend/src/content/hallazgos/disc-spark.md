**Se beneficiaron de verdad de Spark:**

- **La ingesta.** Parsear 279 mil documentos JSON anidados (~3 GB), desanidar ítems, adjudicaciones, proveedores y contratos, deduplicar, convertir monedas y escribir seis tablas Parquet particionadas toma 5,5 minutos en frío en el clúster local de 4 núcleos (113 s solo el parseo). Spark lo hace en paralelo con memoria acotada por tarea. En pandas habría que escribir a mano un parseo por lotes para no cargar 3 GB de objetos Python en memoria.
- **LSH sobre todo el corpus.** Las firmas por partición, los buckets por banda y el self-join de candidatos sobre 196 mil procesos tardan unos 44 s. Es la parte donde la distribución es natural y donde una sola máquina, sin LSH, tendría que comparar 19 mil millones de pares.
- **FP-Growth sobre las 196 mil canastas** (4 s) y las agregaciones con ventanas del HHI, que vienen listas en MLlib y Spark SQL.

**Eran manejables en local, y así se hicieron:**

- **ANN:** TF-IDF, SVD y los índices faiss viven en la memoria de una máquina (la matriz ocupa 200 MB) y las consultas tardan fracciones de milisegundo. Spark no tiene índices ANN nativos y distribuirlos no tendría sentido a este tamaño.
- **Las estructuras de flujo:** procesan un evento a la vez, como lo haría un sistema en tiempo real. Spark solo prepara y ordena el flujo.
- **La verdad exacta de la muestra** (Jaccard de 1,1 millones de pares con una multiplicación de matrices dispersas) y Apriori de referencia.

**¿Valió el overhead?** Depende de la tarea, y lo medimos:

- Para una agregación simple, no. Pandas agrupa los 196 mil procesos en 1,1 s y Spark tarda 4,7 s, con un costo fijo casi independiente del tamaño (4,7 s con el 10 % y con el 100 %). El DataFrame es, en cambio, entre 2,4 y 4,8 veces más rápido que el RDD dentro de Spark.
- Para la ingesta y LSH, sí: son las etapas que escalan peor y las que, con 10 o 100 veces más datos, dejarían de caber en una máquina.
- Para el proyecto en su conjunto, el overhead se paga con caché. Una corrida rápida sobre todos los datos reutiliza el parseo y el procesamiento y tarda unos 8 minutos, casi todos en las mediciones y en ANN, no en Spark.

**Conclusión honesta.** Con 196 mil procesos, este proyecto podría hacerse casi entero en pandas en una laptop con 16 GB, salvo la ingesta cómoda del JSON y LSH sobre el corpus completo. Spark se justifica por esas dos tareas y por la escalabilidad: el mismo código procesaría el histórico completo del OECE desde 2015 o el de varios países sin reescribirse.
