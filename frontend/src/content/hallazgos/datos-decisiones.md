**La regla: cada tarea usa el volumen que su pregunta necesita.** Se muestrea donde la muestra conserva la conclusión y ahorra tiempo de verdad, y no donde cambia la respuesta. El enunciado lo permite ("un subconjunto representativo es válido si el volumen total no es manejable, siempre que se justifique"), así que la justificación tiene que ser medida.

**Q1, Q2 y Q3 usan todos los datos, porque ninguna muestra conserva la respuesta.** Se probaron dos diseños:

- **Procesos al azar.** Con el 10 %, solo 241 de las 1 765 entidades evaluables de 2025 conservan 5 adjudicaciones, y el HHI mediano sube de 2 016 a 2 822: con pocas adjudicaciones por entidad el índice se infla. Cuatro de cada cinco entidades que la muestra marca como concentradas no lo son, y solo detecta el 4 % de las que sí. En la Q3, un par sobrevive solo si entran sus dos procesos (p²): queda el 1,1 % de los pares.
- **Entidades completas (conglomerados).** Es la alternativa fuerte, y no sesga: el HHI de cada entidad elegida es exacto, el mediano coincide con el total y no hay falsas alarmas. Pero la Q1 pregunta **cuáles** entidades dependen de pocos proveedores, y con el 10 % de las entidades solo puede encontrar el 10 % de ellas. En la Q3 conserva las repeticiones dentro de cada entidad elegida, pero las que cruzan entidades se siguen perdiendo como p² (0,8 %), y el resultado depende de si cae o no en la muestra el OEFA, que aporta 35 mil de los 130 mil pares.

**Y no muestrear casi no cuesta.** Concentración, EDA, LSH con la clasificación de la Q3, flujos y FP-Growth suman unos 4 minutos sobre los 196 mil procesos. Muestrear al 10 % ahorraría unos 3 minutos y cambiaría las respuestas.

**Dónde sí se muestrea, con su justificación.**

- **Jaccard exacto** (1 500 procesos de entidades completas): es cuadrático y solo sirve de referencia para medir el error de MinHash y LSH. Se eligieron entidades completas para que haya pares similares, y por eso su precisión no se traslada al corpus (86 % en la muestra frente a 66 % en el corpus).
- **Apriori** (10 000 canastas): solo verifica que FP-Growth encuentra lo mismo.
- **ANN en el perfil rápido** (50 mil procesos): compara algoritmos y no responde una pregunta sobre el Estado. El recall con 50 mil (99 %) es algo mayor que con todo (97 %), y la ventaja de HNSW crece con el corpus. Las cifras del informe salen del perfil completo.

**Lo que ahorran los perfiles y las cachés.** La validación y el parseo del JSON se reutilizan si los archivos y el código no cambiaron, y las mediciones de una vez (benchmark RDD/DataFrame, particionamiento, Spark frente a pandas, escalamiento de ANN, efecto de muestrear) solo se repiten cuando se pide. La corrida completa tarda unos 20 minutos con caché; una corrida rápida sobre todos los datos, unos 8. La muestra del 3 % de desarrollo queda para probar cambios de código, no para producir cifras.
