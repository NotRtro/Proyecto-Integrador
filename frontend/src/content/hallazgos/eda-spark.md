**El DataFrame es entre 2,4 y 4,8 veces más rápido que el RDD**, con el mismo resultado verificado en las tres operaciones. El monto por categoría tarda 0,30 s frente a 1,43 s, el conteo por entidad 0,34 s frente a 1,22 s, y el HHI de proveedores por departamento (un join y dos agregaciones) 2,5 s frente a 6,0 s.

**Por qué el RDD es más lento.**

- **Serialización JVM–Python.** En un RDD de PySpark cada `map` y cada `lambda` corre en un proceso Python aparte. Cada fila sale de la JVM, se serializa con pickle, se procesa en Python y vuelve. En un DataFrame, `groupBy().agg()` se ejecuta entero dentro de la JVM y Python solo describe el plan.
- **Catalyst.** El optimizador del DataFrame reescribe la consulta. En el HHI eligió un `BroadcastHashJoin`: copió la tabla pequeña de procesos a cada ejecutor y evitó redistribuir la grande. El RDD hace un `join` genérico que reparte ambos lados por la red, y por eso necesita 3 shuffles frente a 2.
- **Tungsten.** El DataFrame guarda las filas en formato binario compacto y genera código especializado para la agregación. El RDD manipula objetos Python, más pesados.

En ambos estilos la agregación es parcial antes del shuffle: el plan del DataFrame muestra `HashAggregate → Exchange → HashAggregate`, que es lo mismo que hace `reduceByKey` con su combinador local. La diferencia no está en el algoritmo MapReduce, sino en quién lo ejecuta.

**Claridad.** Para estas preguntas el DataFrame se lee como la pregunta misma: "agrupa por departamento y proveedor, suma el monto". El HHI ocupa 7 líneas en DataFrame y 16 en RDD, donde hay que armar a mano las tuplas `(clave, valor)`, el join y la normalización de participaciones. El RDD es más expresivo cuando la lógica no es tabular. Este proyecto lo usa donde corresponde: las firmas MinHash se calculan con `mapPartitions` y numpy, porque cada partición procesa un bloque de documentos con código propio que no existe como función SQL.

**Límite.** Las medianas salen de 3 corridas sobre datos en caché, en un clúster local de 2 workers. En un clúster real con red de por medio, el costo de los shuffles extra del RDD sería mayor.
