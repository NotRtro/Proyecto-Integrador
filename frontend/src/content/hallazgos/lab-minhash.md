**El error observado sigue la teoría casi exactamente.** MinHash estima el Jaccard como la proporción de las k componentes en que coinciden dos firmas, y su error estándar teórico es √(J(1−J)/k). En los 5 778 pares similares de la muestra, el error cuadrático medio observado es 0,115 con k = 16 (teoría: 0,110), 0,051 con k = 64 (0,055), 0,037 con k = 128 (0,039) y 0,028 con k = 256 (0,028). El sesgo es prácticamente cero en todos los casos: el estimador no tiende a sobrestimar ni a subestimar.

**Por qué se mide sobre los pares similares.** De los 1,1 millones de pares de la muestra, la inmensa mayoría tiene Jaccard cero o casi cero, y ahí MinHash acierta trivialmente. El error medio sobre todos los pares (0,001–0,004) sería engañosamente bajo. Lo que importa es el error cerca del umbral de decisión (J ≈ 0,7), donde se separa "casi idéntico" de "parecido".

**Qué k elegimos y por qué: 128.**

- El error baja con 1/√k: duplicar k solo lo reduce un 29 %. Pasar de 128 a 256 baja el error de 0,037 a 0,028, a cambio del doble de memoria y de tiempo de firma.
- Con k = 128, un par con J = 0,70 se estima típicamente entre 0,66 y 0,74, suficiente para decidir si es "casi idéntico".
- 128 permite 16 bandas de 8 filas para LSH, con umbral efectivo ≈ 0,71, justo el umbral de la Q3.
- La firma ocupa 128 enteros por proceso, de tamaño fijo sin importar lo largo del texto: entre 100 y 200 MB para los 196 mil procesos, según se guarden en 32 o 64 bits. Lo caro no es guardarla sino comparar: por eso hace falta LSH.

**El slider de k** muestra el compromiso en vivo: con k pequeño los puntos del gráfico de dispersión se alejan de la diagonal; con k grande se pegan a ella.

**Límite.** La representación son shingles de dos palabras. Dos procesos que dicen lo mismo con otras palabras ("adquisición de combustible" frente a "compra de diésel") tienen Jaccard bajo aunque compren lo mismo. MinHash aproxima bien el Jaccard, pero el Jaccard no mide significado.
