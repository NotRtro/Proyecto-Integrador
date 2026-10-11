**En una agregación simple, pandas gana con todos los datos.** Agrupar los 196 277 procesos por año y categoría tarda 1,1 s en pandas y 4,7 s en Spark. Además el tiempo de Spark casi no cambia entre el 10 % y el 100 % de los datos (4,7, 5,4 y 4,7 s): casi todo es costo fijo de coordinar el trabajo (planificar, lanzar tareas en los ejecutores, recoger resultados), no de procesar filas. La tabla analítica ocupa 32 MB en memoria; cabe holgadamente en una laptop.

**Entonces, ¿por qué Spark?** Por lo que pasa antes de esta tabla y por algunas tareas puntuales:

- **El parseo del JSON anidado.** Los 279 mil documentos OCDS ocupan ~3 GB sin comprimir, con arreglos de ítems, adjudicaciones, proveedores, contratos y documentos. Spark los parsea, desanida y deduplica en paralelo con memoria acotada por partición, y escribe seis tablas Parquet en 5,5 minutos en frío (113 s solo el parseo). En pandas habría que cargar documentos enormes en memoria o escribir a mano un parseo por lotes.
- **LSH sobre todo el corpus.** Armar los buckets de 16 bandas para 196 mil firmas y generar los pares candidatos es un `groupBy` y un self-join que Spark distribuye sin esfuerzo.
- **FP-Growth sobre todas las canastas**, que viene listo en MLlib.

**Lectura honesta.** Con este volumen, Spark no es indispensable para el análisis: lo es para la ingesta y es la arquitectura que escala. El enunciado lo pide explícitamente: el dataset tiene un volumen que "permite argumentar Spark o justificar por qué sigue siendo adecuado". Spark es adecuado porque el mismo código que hoy procesa 3 GB procesaría 30 o 300 GB, mientras que pandas dejaría de caber en memoria en algún punto entre 10 y 100 veces este volumen (la tabla de procesos ya sin texto ocupa 32 MB; con 100 veces, 3,2 GB, y el JSON crudo, 300 GB).

**Límite.** La comparación es de punta a punta (leer Parquet, filtrar, agrupar, traer el resultado) y en un clúster local, donde el overhead de Spark pesa más que en un clúster real con datos grandes.
