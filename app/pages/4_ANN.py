import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction
introduction('Búsqueda de vecinos · ANN','¿Qué aceleración ofrece HNSW y cuánto recall se pierde?','TF-IDF de unigramas y bigramas, máximo 4096 dimensiones, normalización L2. Exacto y HNSW consultan los mismos vectores; se excluye el propio proceso.')
grid=load('ann/hnsw_grid.csv','part3.run'); neighbors=load('ann/neighbors_display.csv','part3.run'); docs=load('ann/documents.csv','part3.run'); queries=load('ann/queries.json','part3.run')
text=st.text_input('Buscar texto en las consultas precalculadas')
choices=[q for q in queries if text.casefold() in q['text'].casefold() or text in q['ocid']]
if not choices: st.info('Ninguna consulta precalculada coincide con el texto.'); st.stop()
q=st.selectbox('Proceso de consulta',[q['ocid'] for q in choices]); st.write(next(x['text'] for x in choices if x['ocid']==q))
config=st.selectbox('Algoritmo y configuración',['exact']+grid.config.tolist()); k=st.selectbox('Vecinos',[5,10])
result=neighbors[(neighbors.query_ocid==q)&(neighbors.config==config)&(neighbors['rank']<=k)].merge(docs[['ocid','text']],left_on='neighbor_ocid',right_on='ocid',how='left'); table(result)
chart(px.scatter(grid,x='median_ms',y='recall_at_10',color='M',hover_data=['efConstruction','efSearch','speedup'],title='Recall@10 frente a latencia mediana (ms)'))
if config!='exact':
    r=grid[grid.config==config].iloc[0]
    st.write(f"Esta configuración conserva {r.recall_at_10:.1%} de los 10 vecinos exactos y tiene una aceleración media de {r.speedup:.2f}×. Su latencia p95 es {r.p95_ms:.3f} ms; construir el índice tomó {r.build_seconds:.2f} s y ocupa {r.index_bytes/1024/1024:.2f} MiB.")
table(grid)
st.caption('Resultados limitados al corpus muestreado y consultas fijas. Descripciones idénticas generan empates: otros vecinos igualmente cercanos pueden reducir recall de identificadores. Una aceleración inferior a 1 significa que la búsqueda exacta fue más rápida. IVF es una extensión opcional no ejecutada.')
