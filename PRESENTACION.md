# Guion de presentación y demostración

Duración sugerida: 15 minutos. Cada integrante presenta y sustenta una parte, y todos deben poder explicar cualquier línea del código.

1. **Problema y preguntas (1 min).** Datos abiertos del OECE en OCDS (2023–2025, 196 277 procesos). Tres preguntas: Q1 concentración de proveedores, Q2 postor único, Q3 requerimientos repetidos. Por qué importan para la contratación pública peruana.
2. **Datos y KDD (2 min).** Página *Datos y KDD*. El año se toma de la convocatoria y no del archivo. Deduplicación por `ocid` y su límite: el hospital de Loreto aparece cuatro veces. Conversión de monedas. El título OCDS es la nomenclatura. Los consorcios se llevan la mitad del monto. Decisiones de volumen: por qué ningún diseño de muestra (procesos al azar ni entidades completas) conserva la respuesta de Q1–Q3. Abrir la Spark UI (http://localhost:8080) con los dos workers.
3. **Spark (2 min).** Página *EDA y Spark*. La transición a la Ley 32069 en la serie mensual de 2025. Código RDD frente a DataFrame lado a lado (broadcast join, 2,4–4,8 veces más rápido). Particionamiento: pruning (12 de 36 carpetas), shuffle partitions y AQE. Por qué pandas gana en una agregación simple y dónde Spark sí hizo falta.
4. **Q1 (2 min).** Las municipalidades son las más dependientes, pero el aumento está inflado por tener menos adjudicaciones. Un HHI alto suele ser una obra (Huancavelica). Loreto se explica con un solo contrato. Proveedores que repiten con la entidad (Bloom).
5. **Q2 (2 min).** "Postor" son participantes registrados (la regla Comparación de Precios → 2–3 postores lo delata). El postor único es casi exclusivo de las modalidades no competitivas; Tumbes y las emergencias del norte. Reglas dirigidas: bienes por Adjudicación Simplificada en municipalidades. DGIM como alerta.
6. **Q3 (2 min).** De 19 mil millones de pares a 198 mil candidatos con LSH. Separar reconvocatorias y pares del mismo procedimiento. OEFA y Ucayali: equipos contratados persona por persona. Casos: obra y supervisión, hospital de Loreto, microcubetas. Buscador de vecinos: "césped artificial" encuentra "grass sintético".
7. **Laboratorio (2 min).** Mover los controles: k de MinHash frente al error, curva S de LSH, FPR de Bloom (la teoría coincide con la misma ocupación), cota de Count-Min, umbrales de reglas. Explicar HNSW e IVF con los diagramas.
8. **Discusión (1 min).** Qué aportó cada técnica, precisión frente a costo, escenario de 10× y 100× e implicancias éticas. Una similitud, una concentración o una regla son señales, no pruebas.
9. **Reproducibilidad (1 min).** README, `make all`, `make test` (pytest, API, render y completitud del contenido), manifiesto con el commit del código, 19 controles de validación y la web publicada.

Para el video: grabar la web y la Spark UI, y narrar solo cifras que aparecen en pantalla.
