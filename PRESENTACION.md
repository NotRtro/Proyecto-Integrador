# Guion de presentación y demostración

1. **Problema (1 min).** Cinco preguntas: concentración, similitud, vecinos, flujos y asociaciones. Datos públicos reales OECE, 2023–2025.
2. **Datos y KDD (2 min).** Mostrar inventario, SHA-256, cobertura efectiva y descartes. Explicar que archivo 2025 no equivale a convocatoria 2025. Mostrar anomalías de ingesta registradas y deduplicación.
3. **Arquitectura (1 min).** Abrir Spark UI y sus dos workers. JSONL → Spark → Parquet → artefactos → API FastAPI → React. Cambiar filtros consulta agregados de la API y no ejecuta Spark.
4. **Concentración (2 min).** Seleccionar año y departamento. Leer top 10 y HHI; comparar evolución y límites de monto adjudicado. Mostrar tiempos RDD/DataFrame y equivalencia.
5. **Similitud (2 min).** Cambiar hashes, bandas y umbral. Explicar TP/FP/FN y revisar dos descripciones. Distinguir candidato de similitud verificada y similitud de irregularidad.
6. **ANN (2 min).** Seleccionar consulta y configuración HNSW. Contrastar con exacto, recall@10 y latencia; explicar empates y costo de construir el índice.
7. **Flujos (2 min).** Mostrar Bloom sin FN, error CMS y ventana DGIM. Comparar memoria con referencias exactas; explicar por qué CMS puede no ahorrar en dominios pequeños.
8. **Asociación (2 min).** Filtrar soporte, confianza y lift. Interpretar reglas seleccionadas; mostrar igualdad Apriori/FP-Growth sobre la misma muestra.
9. **Discusión (1 min).** Qué técnicas aportaron valor y cuáles no; decisiones de muestreo; 10×/100× volumen. No extrapolar causalidad ni atribuir irregularidades.
10. **Reproducibilidad (1 min).** Mostrar README, `make all`, `make test`, documentación `/docs` de la API y manifiesto final.

Para el video: grabar la aplicación local junto con la Spark UI y narrar resultados realmente mostrados. No usar cifras del guion como si fueran resultados; tomar todas las métricas de los artefactos de la ejecución.
