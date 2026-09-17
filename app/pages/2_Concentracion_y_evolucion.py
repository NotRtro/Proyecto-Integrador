import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction
introduction('Concentración y evolución','¿Cómo se distribuye y concentra el valor entre entidades, proveedores, categorías y departamentos?','Agregaciones Spark sobre todos los procesos válidos del periodo. Filtros locales sobre cubos precalculados.')
df=load('eda/filter_cube.csv','part1.run'); suppliers=load('eda/supplier_cube.csv','part1.run')
filters={}
for field,label in [('year','Año'),('month','Mes'),('department','Departamento'),('category','Categoría'),('buyer_name','Entidad')]:
    value=st.sidebar.selectbox(label,['Todos']+sorted(df[field].dropna().unique().tolist()))
    if value!='Todos': filters[field]=value
for field,value in filters.items():
    df=df[df[field]==value]; suppliers=suppliers[suppliers[field]==value]
if df.empty: st.info('No hay procesos para estos filtros.'); st.stop()
labels={'Número de procesos':'processes','Monto convocado (PEN)':'tender_amount_pen','Monto adjudicado (PEN)':'award_amount_pen'}
metric=labels[st.selectbox('Métrica',list(labels))]; top=st.slider('Elementos del ranking',5,50,10)
cols=st.columns(3)
for c,k,label in zip(cols,['processes','tender_amount_pen','award_amount_pen'],['Procesos','Convocado · PEN','Adjudicado · PEN']): c.metric(label,f'{df[k].sum():,.0f}')
monthly=df.groupby(['year','month'],as_index=False)[metric].sum(); monthly['period']=monthly.year.astype(str)+'-'+monthly.month.astype(str).str.zfill(2)
chart(px.line(monthly,x='period',y=metric,markers=True,title='Evolución mensual'))
for key,label in [('category','Categorías'),('department','Departamentos'),('buyer_name','Entidades')]:
    rank=df.groupby(key,dropna=False)[metric].sum().sort_values(ascending=False).head(top).reset_index(); chart(px.bar(rank,x=metric,y=key,orientation='h',title=f'{label} · {metric}'))
rank=df.groupby('buyer_id')[metric].sum().sort_values(ascending=False); total=rank.sum(); shares=rank/total if total else rank*0
curve=shares.cumsum().reset_index(drop=True).reset_index(name='share'); curve['index']+=1
chart(px.line(curve,x='index',y='share',title='Participación acumulada de entidades'))
st.write(f"Para estos filtros, las diez entidades principales concentran {shares.head(10).sum():.1%} de {metric}. El HHI descriptivo es {(shares**2).sum()*10000:,.0f} sobre una escala de 0–10 000.")
years=df.groupby('year')[metric].sum().sort_index()
if len(years)>1 and years.iloc[-2]: st.write(f'El total varía {(years.iloc[-1]/years.iloc[-2]-1):+.1%} entre {years.index[-2]} y {years.index[-1]}. La cobertura temporal puede ser distinta entre años.')
sup=suppliers.groupby('supplier_name',dropna=False).allocated_amount_pen.sum().sort_values(ascending=False).head(top).reset_index(); chart(px.bar(sup,x='allocated_amount_pen',y='supplier_name',orientation='h',title='Proveedores · monto adjudicado atribuido (PEN)'))
st.caption('En adjudicaciones con varios proveedores se reparte el monto equitativamente para no multiplicarlo. No son pagos individuales. La concentración no demuestra irregularidad.')
with st.expander('Distribución de montos y proveedores únicos por entidad'):
    table(load('eda/amount_distribution.csv')); table(load('eda/buyer_supplier_counts.csv'))
bench=load('eda/rdd_dataframe_benchmark.csv'); chart(px.bar(bench,x='operation',y='median_seconds',color='engine',barmode='group',title='RDD vs DataFrame · mediana de tres ejecuciones (s)')); table(bench)
st.write('La comparación incluye la misma agregación y verifica resultados equivalentes. El tiempo observado depende del volumen, particiones, caché y recursos del equipo.')
with st.expander('Planes de ejecución y shuffle'): st.json(load('eda/execution_plans.json'))
