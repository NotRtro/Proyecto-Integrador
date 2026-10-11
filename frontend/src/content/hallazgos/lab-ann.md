**Cómo funcionan, en palabras simples.**

- **HNSW** es como la red de carreteras de un país. Para llegar de Lima a un pueblo de Cusco no se recorren todos los caminos: se toma una autopista (la capa superior, con pocos nodos y saltos largos), luego una carretera regional y al final un camino local (la capa 0, donde están todos los vectores). Cada vector se conecta con sus M vecinos más cercanos. `efSearch` es cuántos caminos candidatos se mantienen abiertos mientras se baja: más candidatos, más recall y más latencia.
- **IVF** es como buscar a alguien por su distrito. Primero k-means divide el espacio en `nlist` celdas (distritos); cada vector se guarda en la lista de su centroide. Una consulta solo revisa las `nprobe` celdas más cercanas. Si el vecino buscado vive justo al otro lado del límite, se pierde, salvo que se revisen más celdas.

**Resultados sobre los 196 mil procesos (200 consultas, IC del 95 % por bootstrap).** La búsqueda exacta tarda 21,6 ms por consulta.

- **HNSW** con M = 16, efConstruction = 128 y efSearch = 64 recupera el 96,9 % de los 10 vecinos exactos (IC 95,0–98,5 %) en 0,18 ms, unas 120 veces más rápido. Con efSearch = 16 baja a 95 % y se acerca a 250 veces. Duplicar efConstruction mejora el grafo (+1 a 3 puntos de recall) a cambio del doble de tiempo de construcción (de 14 a 26 s).
- **IVF** depende sobre todo de nprobe. Con nlist = 1 024, revisar 1 celda da 76 % de recall; 4 celdas, 93 %; 16 celdas, 97 % en 0,55 ms (39 veces más rápido). Su construcción tarda 74 s por el k-means, frente a menos de 1 s con nlist = 64.

**El escalamiento explica cuándo vale la pena.** Al pasar de 5 000 a 196 000 procesos, la búsqueda exacta crece de 0,5 a 21 ms, casi en proporción al tamaño (O(N)), mientras HNSW se mantiene alrededor de 0,1 ms (crecimiento logarítmico). Con 5 000 procesos HNSW apenas gana 6 veces; con 196 mil, más de 100. La ventaja crece con el corpus, y por eso se evalúa sobre el corpus completo.

**Qué configuración recomendamos.** HNSW con M = 16, efConstruction = 128 y efSearch entre 32 y 64: recall cercano al 97 %, menos de 0,2 ms por consulta y un índice de 229 MB, apenas mayor que la matriz exacta (201 MB). IVF conviene si el índice se reconstruye seguido (con nlist bajo se construye en segundos) o si la memoria es más estrecha.

**Por qué el recall no llega a 100 % aunque los procesos sean casi idénticos.** Hay cientos de textos repetidos (las plantillas del OEFA, por ejemplo). Cuando 50 procesos tienen la misma similitud con la consulta, los "10 vecinos exactos" son una elección arbitraria entre empates, y un índice aproximado puede devolver otros 10 igual de parecidos. El recall literal castiga eso aunque la respuesta sea igual de buena.

**Límite.** Las latencias son de una consulta a la vez, en un hilo y en una laptop con otros procesos activos, así que varían entre corridas (de 0,08 a 0,35 ms para configuraciones parecidas). El orden de magnitud y la tendencia son estables.
