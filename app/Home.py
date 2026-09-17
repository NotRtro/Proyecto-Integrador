import streamlit as st
from app.data_loader import ROOT,load
st.set_page_config(page_title='Observatorio OECE',page_icon='🇵🇪',layout='wide')
st.title('Contrataciones públicas del Perú')
st.markdown('### Minería de datos · 2023–2025')
st.write('Explora la distribución de las contrataciones, descripciones similares, búsqueda de vecinos, resúmenes de flujos y reglas de asociación con datos reales del OECE.')
st.info('Selecciona un módulo en el menú lateral. Los cálculos se ejecutan fuera de línea; los filtros consultan resultados ya generados.')
st.markdown('**Arquitectura:** JSONL → Spark (dos workers) → Parquet → experimentos → artefactos → Streamlit.')
for q in ['¿Dónde se concentra el valor de las contrataciones?', '¿Qué descripciones son similares y con qué precisión las detecta LSH?', '¿Cuánto acelera HNSW la búsqueda y qué recall conserva?', '¿Qué errores y consumo de memoria tienen Bloom, CMS y DGIM?', '¿Qué características aparecen juntas con mayor frecuencia?']:
    st.write('• '+q)
st.markdown('[Fuente: Registro OCDS del OECE](https://data.open-contracting.org/es/publication/135). Licencia CC BY 4.0 según el plan del proyecto.')
st.warning('El año del archivo corresponde a la publicación o actualización del registro y puede contener procesos históricos. El análisis usa la fecha efectiva de convocatoria.')
if (ROOT/'metadata/run_manifest.json').exists():
    r=load('metadata/run_manifest.json'); st.metric('Estado del último pipeline',r['status']); st.json(r,expanded=False)
else:
    st.code('make all')
