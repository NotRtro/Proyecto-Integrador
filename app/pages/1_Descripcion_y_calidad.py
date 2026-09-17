import pandas as pd
import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction,selector
introduction('Descripción y calidad','¿Qué datos entraron al análisis y qué se descartó?','KDD: validar JSON → normalizar OCDS → deduplicar ocid → filtrar fecha de convocatoria → tablas Parquet.')
manifest=load('metadata/source_manifest.json'); quality=load('metadata/quality_summary.json')
cols=st.columns(3)
cols[0].metric('Registros originales',f"{quality['raw_records']:,}"); cols[1].metric('Procesos analizados',f"{quality['selected_processes']:,}"); cols[2].metric('Versiones duplicadas',f"{quality['duplicate_ocids']:,}")
table(pd.DataFrame(manifest))
coverage=load('metadata/temporal_coverage.csv'); chart(px.bar(coverage,x='effective_year',y='count',color='file',title='Cobertura por año efectivo · registros'))
nulls=selector(load('metadata/nulls_by_year.csv'),'year','Año'); chart(px.bar(nulls,x='field',y='percentage',color='year',barmode='group',title='Valores ausentes · porcentaje'))
st.write(f"Se conservaron {quality['selected_processes']:,} procesos entre {quality['min_date']} y {quality['max_date']}; {quality['outside_period_or_missing_date']:,} procesos únicos quedaron fuera del periodo o carecen de fecha utilizable.")
st.write('Los porcentajes de nulos indican qué variables limitan cada comparación; ausencia de monto no se interpreta como contratación de valor cero.')
st.json(quality,expanded=False)
with st.expander('Diccionario y esquema analítico'): st.json(load('metadata/analytical_schema.json'))
st.caption('Departamento = ubicación de la entidad compradora, no necesariamente lugar de ejecución. Se conserva la versión más reciente de cada ocid. Los montos negativos se excluyen de los totales.')
