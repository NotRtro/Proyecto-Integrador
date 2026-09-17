import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction,selector
introduction('Minería de flujos','¿Con qué error y memoria se resumen eventos temporales?','Un evento por proceso, ordenado por convocatoria y ocid; cada estructura solo recibe eventos presentes o anteriores.')
a,b,c=st.tabs(['Bloom Filter','Count-Min Sketch','DGIM'])
with a:
    data=load('streaming/bloom_grid.csv','part4.run'); data=selector(data,'bits','Bits'); data=selector(data,'hashes','Hashes'); table(data)
    chart(px.bar(data,x='bits',y=['observed_fpr','theoretical_fpr'],barmode='group',title='Falsos positivos · tasa observada y teórica final'))
    st.write(f"Se observaron {int(data.fn.sum())} falsos negativos en las configuraciones seleccionadas. Bloom responde si una entidad pudo haber aparecido: un positivo puede ser falso; un negativo es definitivo.")
    st.caption('La tasa observada se mide antes de insertar y cambia con la ocupación; la fórmula teórica usa la ocupación final. No son estimaciones del mismo instante.')
with b:
    data=load('streaming/cms_grid.csv','part4.run'); data=selector(data,'width','Ancho'); data=selector(data,'depth','Profundidad'); data=selector(data,'value','Categoría'); table(data)
    chart(px.bar(data,x='value',y=['exact','estimate'],barmode='group',title='Frecuencia exacta y estimada · eventos'))
    if not data.empty: st.write(f'La sobreestimación máxima seleccionada es {data.absolute_error.max():,.0f} eventos. CMS no subestima; las colisiones solo aumentan los conteos. El dominio reducido de categorías puede producir error cero.')
with c:
    data=load('streaming/dgim_grid.csv','part4.run'); w=st.selectbox('Ventana',sorted(data.window.unique())); t=st.selectbox('Umbral de alto monto · PEN',sorted(data.threshold.unique())); data=data[(data.window==w)&(data.threshold==t)]; table(data)
    series=load('streaming/error_over_time.csv','part4.run'); series=series[(series.window==w)&(series.threshold==t)]
    if not series.empty: chart(px.line(series,x='event',y=['exact','estimate'],title='Procesos de alto monto en los últimos N eventos'))
    if not data.empty:
        r=data.iloc[0]; st.write(f'DGIM estima {r.estimate:,.1f} procesos frente a {r.exact:,.0f} exactos: error absoluto {r.absolute_error:,.1f}, utilizando {r.buckets:,.0f} buckets.')
    st.caption('Ventana de eventos, no días. Los umbrales son fijos y no usan cuantiles del futuro.')
memory=load('streaming/memory_comparison.csv'); chart(px.bar(memory,x='config',y=['approx_bytes','exact_bytes'],facet_row='algorithm',barmode='group',title='Memoria estimada · bytes')); st.write('El ahorro depende de la configuración y cardinalidad. Con pocas categorías, el contador exacto puede ocupar menos que CMS.')
with st.expander('Decisiones de medición'): st.json(load('streaming/methodology.json'))
