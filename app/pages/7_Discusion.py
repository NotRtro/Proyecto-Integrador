import pandas as pd
import streamlit as st
from app.data_loader import load,table,introduction
introduction('Discusión y límites','¿Cuándo justifican las aproximaciones su pérdida de precisión?','Resultados observados, interpretación, limitaciones y decisiones metodológicas se presentan por separado.')
lsh=load('similarity/lsh_parameter_grid.csv','part2.run'); ann=load('ann/hnsw_grid.csv','part3.run'); bloom=load('streaming/bloom_grid.csv','part4.run'); cms=load('streaming/cms_grid.csv','part4.run'); dgim=load('streaming/dgim_grid.csv','part4.run')
best=lsh.sort_values('f1',ascending=False).iloc[0]; fastest=ann.sort_values('speedup',ascending=False).iloc[0]
st.subheader('Resultado observado')
st.write(f"LSH alcanzó F1 máximo {best.f1:.3f} con precisión {best.precision:.1%} y recall {best.recall:.1%}. HNSW obtuvo aceleración máxima {fastest.speedup:.2f}× con recall@10 {fastest.recall_at_10:.1%}. Estos extremos no necesariamente pertenecen a la configuración más conveniente.")
table(pd.DataFrame([
 {'Técnica':'Jaccard exacto','Exactitud':'Referencia','Tiempo':f'{best.exact_seconds:.3f} s','Memoria':'O(n²) pares en evaluación','Escalabilidad':'Baja','Valor':'Verdad de referencia'},
 {'Técnica':'MinHash LSH','Exactitud':f'F1 {best.f1:.3f}','Tiempo':f'{best.lsh_seconds:.3f} s candidatos','Memoria':'O(n × hashes + candidatos)','Escalabilidad':'Distribuida','Valor':'Reducir pares a verificar'},
 {'Técnica':'Fuerza bruta','Exactitud':'Referencia coseno','Tiempo':f'{fastest.exact_mean_ms:.3f} ms/consulta','Memoria':'Vectores TF-IDF','Escalabilidad':'Lineal por consulta','Valor':'Vecinos exactos'},
 {'Técnica':'HNSW','Exactitud':f'Recall {fastest.recall_at_10:.3f}','Tiempo':f'{fastest.mean_ms:.3f} ms/consulta','Memoria':f'{fastest.index_bytes/1024/1024:.2f} MiB índice','Escalabilidad':'Índice en RAM','Valor':'Recuperación aproximada'},
 {'Técnica':'Bloom','Exactitud':f'{int(bloom.fn.sum())} FN; FPR variable','Tiempo':'Ver rejilla','Memoria':'Bits fijos','Escalabilidad':'Flujo','Valor':'Pertenencia'},
 {'Técnica':'CMS','Exactitud':f'Error máximo {cms.absolute_error.max():.0f}','Tiempo':'Ver rejilla','Memoria':'Ancho × profundidad','Escalabilidad':'Flujo','Valor':'Frecuencias'},
 {'Técnica':'DGIM','Exactitud':f'Error máximo final {dgim.absolute_error.max():.1f}','Tiempo':'Ver rejilla','Memoria':'O(log ventana) buckets','Escalabilidad':'Flujo','Valor':'Conteos recientes'},
]))
st.subheader('Interpretación')
st.write('EDA aporta la respuesta más directa sobre concentración; LSH y ANN ayudan a explorar descripciones. Su valor depende del recall exigido y del volumen: una muestra pequeña puede favorecer la búsqueda exacta. CMS aporta poco si el dominio de categorías cabe en un diccionario pequeño.')
st.subheader('Decisión metodológica')
st.write('Spark procesa archivos anidados, deduplicación, agregaciones, bandas LSH y FP-Growth. Los experimentos ANN y Apriori usan muestras deterministas y se ejecutan en el driver. Los sketches recorren el flujo ordenado; Spark distribuye su preparación. La web solo consume artefactos.')
st.subheader('Limitaciones y escalamiento')
st.write('Con 10× datos conviene aumentar particiones, almacenamiento y medir el cuello de botella. Con 100×, evitar recolectar candidatos o itemsets en el driver, particionar índices ANN, limitar canastas y publicar agregados paginados. Los límites de muestra actuales controlan memoria, pero no prueban escalabilidad a ese volumen.')
st.write('Los registros recompilados describen el último estado disponible, no una fotografía histórica. Montos convocados/adjudicados no equivalen a gasto ejecutado; monedas sin conversión quedan fuera de los totales. La localización corresponde al comprador. Similitud, concentración y asociación no acreditan irregularidad.')
with st.expander('Validación y trabajo académico pendiente'): st.json(load('metadata/artifact_validation.json'))
