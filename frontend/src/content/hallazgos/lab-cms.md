**Dos dominios con muchas claves.** El sketch cuenta adjudicaciones por proveedor (161 255 eventos y 70 739 proveedores distintos) y pedidos por código UNSPSC (194 202 ítems y 17 377 códigos). La garantía se cumple en todas las configuraciones: el sketch nunca subestima, y la proporción de claves con error menor que ε·N (ε = e/w) siempre supera la probabilidad prometida 1 − e^(−d).

**El error es pequeño en las claves frecuentes y enorme en la cola.** Con un sketch de 2 048 columnas (48–80 KB según tenga 3 o 5 filas):

- Los códigos UNSPSC más pedidos se estiman con un 4 % de error relativo medio. El más frecuente, con 6 316 ítems (el código del diésel), se estima en 6 333.
- El proveedor con más adjudicaciones (Grupo Santa Fe, 1 520) se estima en 1 574, y el segundo (Diagnóstica Peruana, 477, el mismo de las microcubetas de la Q3) en 518.
- En la mitad menos frecuente, el error relativo medio es de 4 700 % en proveedores y 1 600 % en UNSPSC: un proveedor que ganó una vez aparece con decenas.

**Por qué la cola sale tan mal.** Cada celda del sketch suma todas las claves que caen en ella. El error esperado de cualquier clave es del orden de N/w: unas 80 adjudicaciones con w = 2 048, sin importar su frecuencia real. Para una clave con 1 500 eventos eso es un 5 %; para una con 1, es 8 000 %. El mínimo sobre d filas reduce el error, pero no cambia su escala. En proveedores la cola es más larga (el 74 % de los 70 mil proveedores ganó una sola vez y el 84 %, una o dos) y el error es mayor que en UNSPSC, donde la demanda está más concentrada en pocos códigos.

**¿Sketch o diccionario?** Con estos volúmenes, un diccionario exacto ocupa 8,5 MB para proveedores y 2 MB para UNSPSC: cabe sin problema, y en este proyecto el sketch es didáctico. El sketch conviene cuando las claves son millones (pares entidad–proveedor–mes, o un flujo continuo durante años en un servidor con memoria fija) y la consulta es sobre los **frecuentes**: "¿qué proveedores están acumulando muchas adjudicaciones este mes?" o "¿qué productos se están pidiendo más?". Para auditar a un proveedor puntual con pocas adjudicaciones, el sketch no sirve.

**Configuración que elegiríamos: 8 192 × 5 (320 KB)** si interesa el top 50 de proveedores (error medio de 7 % en los 100 primeros, frente a 47 % con 2 048 × 5). Para UNSPSC basta 2 048 × 3 (48 KB, error de 6 % en los frecuentes).

**Límite.** El costo por actualización (9–13 µs en Python) crece con d. En un sistema real se implementaría en un lenguaje compilado y sería mucho más rápido.
