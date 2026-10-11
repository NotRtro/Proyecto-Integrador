# Guion de presentación y demostración

Duración sugerida: 15 minutos. Cada integrante presenta y sustenta una parte; todos deben poder explicar cualquier línea del código.

1. **Problema y preguntas (1 min).** Datos abiertos del OECE en OCDS (2023–2025). Tres preguntas: Q1 concentración de proveedores, Q2 postor único, Q3 requerimientos repetidos. Por qué importan para la contratación pública peruana.
2. **Datos y KDD (2 min).** Página *Datos y KDD*. Archivo frente a año de convocatoria, deduplicación, conversión de monedas y el hallazgo de que el título OCDS es la nomenclatura del proceso. Consorcios con la mitad del monto. Abrir la Spark UI (http://localhost:8080) con los dos workers.
3. **Spark (2 min).** Página *EDA y Spark*. Código RDD frente a DataFrame lado a lado; experimento de particionamiento (pruning, shuffle partitions, AQE); por qué pandas gana en una agregación simple y dónde Spark sí hizo falta.
4. **Q1 (2 min).** El HHI nacional es bajo, pero las municipalidades dependen cada vez más de pocos proveedores. Mostrar la tasa de proveedores que repiten con la misma entidad y su relación con el Bloom Filter.
5. **Q2 (2 min).** Tasa de postor único por modalidad. Las modalidades no competitivas por diseño se excluyen. Las reglas con mayor lift y el conteo reciente con DGIM.
6. **Q3 (2 min).** De 19 mil millones de pares a unos 200 mil candidatos con LSH. Quién repite requerimientos y casos revisados en SEACE. Buscador de vecinos con HNSW.
7. **Laboratorio (2 min).** Mover los controles: k de MinHash frente al error, curva S de LSH, FPR de Bloom, cota de Count-Min. Explicar HNSW e IVF con los diagramas.
8. **Discusión (1 min).** Precisión frente a costo de cada técnica, escenario de 10× y 100× e implicancias éticas. Una similitud, una concentración o una regla son señales, no pruebas.
9. **Reproducibilidad (1 min).** README, `make all`, `make test`, manifiesto con el commit del código, validación con 18 controles, web publicada.

Para el video: grabar la web y la Spark UI y narrar solo cifras que aparecen en pantalla. Las interpretaciones deben ser las que el equipo escribió en `frontend/src/content`.
