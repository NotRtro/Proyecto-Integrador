**Lo que más aportó.**

- **Las agregaciones en Spark (Q1 y Q2).** Son las técnicas más simples y las que más respuestas dieron: el HHI por entidad mostró que la dependencia está en las municipalidades, que su aumento está inflado por un efecto mecánico y que la concentración departamental la explican unas pocas megaobras. Las tasas por modalidad mostraron que el postor único es casi exclusivo de las modalidades no competitivas, y llevaron a Tumbes y a las emergencias del norte.
- **MinHash + LSH (Q3).** Sin LSH la Q3 no se puede responder: 19 mil millones de pares no se comparan en una laptop. Con LSH aparecieron el patrón del OEFA y del proyecto de titulación de Ucayali (equipos de proyecto contratados persona por persona), el hospital de Loreto licitado dos veces (que a su vez explicó la concentración de Loreto en la Q1) y los procesos con varios `ocid`, que son un problema de calidad del registro.
- **Las reglas dirigidas (Q2),** junto con la clasificación normativa/informativa y el improvement. Sin esos filtros, las reglas con mayor lift solo repetían la ley. Con ellos aparece el perfil de riesgo (bienes por Adjudicación Simplificada en municipalidades). La regla "Comparación de Precios → 2–3 postores" fue la clave para entender que el campo de postores cuenta participantes y no ofertas.

**Lo que aportó en parte.**

- **HNSW e IVF.** El buscador de vecinos es útil para un comprador o un auditor y encontró sinónimos que el Jaccard no ve ("césped artificial" y "grass sintético"), pero no respondió por sí solo ninguna de las tres preguntas: complementa a LSH.
- **Bloom Filter y DGIM.** Funcionan como prototipos de vigilancia (proveedores que repiten con la entidad, alertas de postor único después de una emergencia), y la tasa de repetición sí alimentó la Q1. Con 160 mil eventos, la memoria que ahorran no era necesaria.

**Lo que aportó poco.**

- **Count-Min Sketch.** Estima bien a los proveedores y productos más frecuentes, pero con 70 mil proveedores un diccionario exacto ocupa 8,5 MB y responde sin error. Para la Q1, que necesita a cada proveedor de cada entidad, el sketch es peor que la alternativa exacta. Su valor aparecería en un flujo continuo y mucho mayor.
- **Apriori.** Solo sirvió para verificar que FP-Growth encuentra exactamente los mismos conjuntos.

**La lección.** El valor vino de combinar técnicas sobre las mismas preguntas: el hospital de Loreto aparece en la Q1 (concentración), en la Q3 (repetición) y en la calidad de datos (copias), y solo cruzándolas se entiende qué pasó.
