**La teoría se cumple, siempre que se compare lo comparable.** El flujo tiene 161 255 adjudicaciones y 112 323 pares entidad–proveedor distintos. La fórmula (1 − e^(−kn/m))^k da la tasa de falsos positivos de un filtro con n elementos insertados. Hay dos maneras correctas de contrastarla:

- **En el flujo**, cada consulta se hace con el filtro a medio llenar. El FPR observado acumulado se compara con la fórmula evaluada con la ocupación de cada consulta. Con 64 KB y k = 3: 3,23 % observado frente a 3,21 % teórico.
- **Al final**, con el filtro lleno, se consultan 20 000 claves que nunca se insertaron. Con 64 KB y k = 3: 11,0 % observado frente a 10,7 % teórico.

En las nueve configuraciones la diferencia es menor a medio punto. Una primera versión del proyecto comparaba el FPR del flujo con la fórmula del filtro lleno y mostraba un observado que era la mitad del teórico (37 % frente a 79 % con 16 KB). No era un error del filtro, sino de la comparación: mezclaba dos ocupaciones distintas. Ninguna configuración tuvo falsos negativos, como garantiza la estructura.

**Qué tamaño elegiríamos.** Para "¿este proveedor ya ganó con esta entidad?" sobre tres años de adjudicaciones:

- **16 KB** no sirve: lleno, responde "ya lo vi" al 79–98 % de los pares nuevos.
- **64 KB** sirve como filtro previo. Durante el flujo se equivoca en el 3 % de los pares nuevos, pero al final en el 11–17 %.
- **256 KB con k = 3–5** es la elección: un FPR final de 0,07–0,3 %, prácticamente exacto, con 50 veces menos memoria que el conjunto de Python (13,6 MB).

**Sobre k.** El k óptimo es (m/n)·ln 2: 1 para 16 KB, 3 para 64 KB y 13 para 256 KB. En el filtro de 64 KB, k = 3 es mejor que 5 o 7 con el filtro lleno, como predice la fórmula: más hashes encienden más bits y saturan el filtro. Con 256 KB sobra espacio y más hashes reducen el FPR, aunque cada operación cuesta más.

**Qué se ahorra.** El conjunto exacto de 112 mil pares ocupa 13,6 MB en Python. Con este volumen cabe sin problema, así que aquí el Bloom Filter es didáctico. Vale la pena cuando el estado no cabe en memoria: por ejemplo, un servicio que verifique en tiempo real todos los pares entidad–proveedor–producto de 20 años de contrataciones, o que comparta el filtro entre muchos procesos.
