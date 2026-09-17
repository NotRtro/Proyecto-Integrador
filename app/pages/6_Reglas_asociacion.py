import math
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from app.data_loader import load,table,chart,introduction,selector
introduction('Reglas de asociación','¿Qué características aparecen juntas con mayor frecuencia?','Una canasta por proceso: año, categoría, método, departamento, monto y competencia. Apriori se compara con FP-Growth sobre la misma muestra; FP-Growth también analiza el conjunto completo.')
rules=load('association/rules.csv','part5.run')
if rules.empty: st.info('No se encontraron reglas que superen los criterios.'); st.stop()
year=st.selectbox('Año',['Todos']+sorted([str(v) for v in rules.year.unique() if str(v)!='Todos'])); rules=rules[rules.year.astype(str)==year]
support=st.slider('Soporte mínimo',.0,.5,.02,.01); confidence=st.slider('Confianza mínima',.0,1.,.5,.05); lift=st.slider('Lift mínimo',1.,10.,1.01,.01)
field=st.selectbox('Campo del antecedente',['Todos','CAT=','METODO=','DEP=','MONTO=','POSTORES=','ANIO=']); size=st.slider('Tamaño máximo de regla',2,6,4)
rules=rules[(rules.support>=support)&(rules.confidence>=confidence)&(rules.lift>=lift)&(rules['size']<=size)]
if field!='Todos': rules=rules[rules.antecedent.str.contains(field,regex=False)]
rules=rules.sort_values(['lift','support'],ascending=False); table(rules)
if not rules.empty:
    chart(px.scatter(rules,x='support',y='confidence',color='lift',hover_data=['antecedent','consequent'],title='Soporte, confianza y lift'))
    r=rules.iloc[0]; st.write(f"La regla más fuerte bajo estos filtros vincula {r.antecedent} con {r.consequent}: soporte {r.support:.1%}, confianza {r.confidence:.1%} y lift {r.lift:.2f}.")
selected=load('association/selected_rules.json'); st.subheader(f"Reglas interpretadas: {selected['found']} de 8")
for i,r in enumerate(selected['rules'],1): st.write(f"{i}. {r['interpretation']}")
nodes=sorted({r[k] for r in selected['rules'] for k in ['antecedent','consequent']})
if nodes:
    pos={n:(math.cos(2*math.pi*i/len(nodes)),math.sin(2*math.pi*i/len(nodes))) for i,n in enumerate(nodes)}; fig=go.Figure()
    for r in selected['rules']:
        a,b=pos[r['antecedent']],pos[r['consequent']]; fig.add_annotation(x=b[0],y=b[1],ax=a[0],ay=a[1],xref='x',yref='y',axref='x',ayref='y',showarrow=True,arrowhead=2)
    fig.add_trace(go.Scatter(x=[pos[n][0] for n in nodes],y=[pos[n][1] for n in nodes],mode='markers',text=nodes,hoverinfo='text',marker={'size':18})); fig.update_layout(title='Grafo de reglas seleccionadas · antecedente → consecuente'); chart(fig)
bench=load('association/algorithm_benchmark.csv'); chart(px.bar(bench,x='support',y='seconds',color='algorithm',facet_col='scope',barmode='group',title='Apriori y FP-Growth · tiempos reales (s)')); table(bench)
st.warning('Asociación no implica causalidad ni irregularidad. Los rangos de monto son decisiones de discretización; las reglas reflejan también composición institucional y cobertura. El soporte mínimo de minería fue 2 % global: bajar el filtro no descubre reglas no calculadas.')
