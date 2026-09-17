import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction
introduction('Similitud LSH','¿Qué descripciones son similares y con qué precisión las recupera LSH?','Shingles de dos palabras → firmas MinHash → bandas LSH ejecutadas en Spark. Referencia exacta: todos los pares de la misma muestra reproducible.')
grid=load('similarity/lsh_parameter_grid.csv','part2.run'); pairs=load('similarity/similar_pairs.csv','part2.run'); docs=load('similarity/documents.csv','part2.run')
h=st.selectbox('Funciones hash',sorted(grid.hashes.unique())); g=grid[grid.hashes==h]
r=st.selectbox('Filas por banda',sorted(g.rows.unique())); g=g[g.rows==r]
t=st.select_slider('Umbral Jaccard',options=sorted(g.threshold.unique())); row=g[g.threshold==t].iloc[0]
chart(px.scatter(grid,x='recall',y='precision',color='hashes',size='candidates',hover_data=['bands','rows','threshold'],title='Precisión y recall de candidatos LSH'))
st.write(f"Con {int(h)} hashes, {int(row.bands)} bandas y umbral {t:.2f}, LSH propone {int(row.candidates):,} pares: precisión {row.precision:.1%}, recall {row.recall:.1%} y F1 {row.f1:.3f}. Hay {int(row.truth_pairs)} pares verdaderos en la muestra.")
st.write(f"Jaccard exacto: {row.exact_seconds:.3f} s. Generación distribuida de candidatos: {row.lsh_seconds:.3f} s (excluye firmas compartidas).")
subset=pairs[pairs.config==row.config]
for field,label in [('left_year','Año de primer proceso'),('left_category','Categoría de primer proceso')]:
    val=st.selectbox(label,['Todos']+sorted(subset[field].dropna().unique().tolist()))
    if val!='Todos': subset=subset[subset[field]==val]
status=st.selectbox('Tipo de par',['TP','FP','FN']); subset=subset[subset.status==status].sort_values('jaccard',ascending=False)
st.caption('Los filtros de año y categoría afectan la tabla; las métricas corresponden a toda la muestra de evaluación.')
table(subset)
if not subset.empty:
    choice=st.selectbox('Comparar par',range(min(100,len(subset))))
    p=subset.iloc[choice]; cols=st.columns(2)
    for col,ocid in zip(cols,[p.left_ocid,p.right_ocid]):
        col.markdown(f'**{ocid}**'); col.write(docs[docs.ocid==ocid].iloc[0].text)
    st.write(f"Jaccard observado: {p.jaccard:.3f}. El solapamiento refleja vocabulario común; revisar el objeto y contexto antes de interpretar duplicidad.")
st.warning('TP/FP/FN evalúan candidatos antes de verificación exacta. Cero pares verdaderos no permite concluir buen recall. Casos sugeridos no sustituyen revisión humana.')
notes=load('similarity/reviewed_cases.json')
for case in notes.get('cases',[]):
    st.subheader('Caso interpretado')
    st.write(case['interpretation']); st.caption(case['limitation'])
with st.expander('Metodología y revisión'): st.json(notes); st.json(load('similarity/methodology.json'))
