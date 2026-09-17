import json
import os
from pathlib import Path
import pandas as pd
import streamlit as st
ROOT=Path(os.getenv('ARTIFACTS_PATH','artifacts'))

@st.cache_data(ttl=30)
def _read(name,stamp):
    p=ROOT/name
    if p.suffix=='.json': return json.loads(p.read_text())
    try: return pd.read_csv(p)
    except pd.errors.EmptyDataError: return pd.DataFrame()

def load(name,stage='run_all'):
    p=ROOT/name
    if not p.exists():
        st.info('Este resultado todavía no fue generado.')
        st.code(f'docker compose run --rm pipeline python -m src.{stage}')
        st.stop()
    return _read(name,p.stat().st_mtime_ns)

def table(df):
    st.dataframe(df,use_container_width=True,hide_index=True)
    st.download_button('Descargar tabla CSV',df.to_csv(index=False).encode(),file_name='resultados.csv',mime='text/csv',key=str(id(df)))

def chart(fig):
    st.plotly_chart(fig,use_container_width=True)
    st.caption('Fuente: OECE, archivos OCDS locales. Montos en PEN cuando existe monto o conversión explícita.')

def introduction(title,question,method):
    st.set_page_config(page_title=title,layout='wide')
    st.title(title); st.markdown(f'**Pregunta:** {question}'); st.caption(method)

def selector(df,column,label=None):
    values=sorted(df[column].dropna().unique().tolist())
    choice=st.selectbox(label or column,['Todos']+values)
    return df if choice=='Todos' else df[df[column]==choice]
