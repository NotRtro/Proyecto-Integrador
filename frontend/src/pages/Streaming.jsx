import { useState } from 'react'
import { api, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, ExecutiveLine, Filters, Loading, SectionHeader, Select, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Streaming() {
  const { data, loading, error } = useAsync(() => api('/api/streaming'), [])
  const [tab, setTab] = useState('Bloom')
  if (loading) return <Loading />
  if (error) return <ErrorState error={error} />
  return <>
    <SectionHeader eyebrow="Pregunta 4" title="Resumir un flujo sin guardar todo"
      description="Tres estructuras aproximadas muestran el intercambio entre memoria, tiempo y error mientras los eventos llegan en orden temporal."
      question="¿Con qué precisión Bloom, Count-Min Sketch y DGIM resumen el flujo?" />
    <div className="tabs">{['Bloom', 'Count-Min Sketch', 'DGIM'].map(name => <button className={tab === name ? 'active' : ''} onClick={() => setTab(name)} key={name}>{name}</button>)}</div>
    {tab === 'Bloom' && <Bloom data={data} />}{tab === 'Count-Min Sketch' && <Cms data={data} />}{tab === 'DGIM' && <Dgim data={data} />}
    <Technical title="Detalle técnico: configuración, memoria y método">
      <h4>Comparación de memoria estimada</h4><DataTable rows={data.memory} />
      <h4>Decisiones metodológicas</h4><pre>{JSON.stringify(data.methodology, null, 2)}</pre>
    </Technical>
  </>
}

function Bloom({ data }) {
  const best = [...data.bloom].sort((a, b) => a.observed_fpr - b.observed_fpr || a.bytes - b.bytes)[0]
  return <><div className="stat-grid four"><Stat label="Falsos negativos" value={data.bloom.reduce((s, r) => s + r.fn, 0)} note="Deben ser cero" /><Stat label="Mejor FPR observado" value={percent(best.observed_fpr, 2)} tone="green" /><Stat label="Memoria" value={`${integer(best.bytes)} bytes`} tone="orange" /><Stat label="Entidades insertadas" value={integer(best.inserted)} tone="navy" /></div><Card title="Tasa de falsos positivos" subtitle="Configuraciones por bits y funciones hash"><ExecutiveBar data={data.bloom.map(r => ({ ...r, label: `${r.bits}b · ${r.hashes}h` }))} category="label" value="observed_fpr" formatter={percent} /></Card><div className="insight"><span>Decisión ejecutiva</span><p>Bloom es apropiado cuando interesa saber si una entidad <em>pudo</em> aparecer y se acepta revisar positivos. Sus negativos son definitivos; los positivos pueden ser falsos.</p></div><Technical title="Rejilla Bloom"><DataTable rows={data.bloom} /></Technical></>
}

function Cms({ data }) {
  const grouped = data.cms.filter(r => r.width === 128 && r.depth === 3)
  const worst = Math.max(...data.cms.map(r => r.absolute_error))
  return <><div className="stat-grid three"><Stat label="Sobreestimación máxima" value={integer(worst)} /><Stat label="Subestimaciones" value="0" note="Garantía conservada" tone="green" /><Stat label="Categorías evaluadas" value={new Set(data.cms.map(r => r.value)).size} tone="orange" /></div><Card title="Exacto frente a estimado" subtitle="Configuración 128 × 3"><ExecutiveBar data={grouped} category="value" value="estimate" formatter={integer} /></Card><div className="insight"><span>Decisión ejecutiva</span><p>Count-Min Sketch es útil para dominios de alta cardinalidad. Con pocas categorías, un diccionario exacto puede ser más pequeño y claro.</p></div><Technical title="Rejilla Count-Min Sketch"><DataTable rows={data.cms} /></Technical></>
}

function Dgim({ data }) {
  const [window, setWindow] = useState(String(data.dgim[0].window)); const [threshold, setThreshold] = useState(String(data.dgim[0].threshold))
  const row = data.dgim.find(r => String(r.window) === window && String(r.threshold) === threshold)
  const series = data.error_over_time.filter(r => String(r.window) === window && String(r.threshold) === threshold)
  return <><Filters><Select label="Ventana de eventos" value={window} onChange={setWindow} options={[...new Set(data.dgim.map(r => String(r.window)))]} all={false} /><Select label="Umbral PEN" value={threshold} onChange={setThreshold} options={[...new Set(data.dgim.map(r => String(r.threshold)))]} all={false} /></Filters><div className="stat-grid three"><Stat label="Conteo exacto" value={integer(row.exact)} /><Stat label="Estimación DGIM" value={Number(row.estimate).toFixed(1)} tone="green" /><Stat label="Error relativo" value={percent(row.relative_error)} note={`${row.buckets} buckets`} tone="orange" /></div><Card title="Conteo de alto monto en la ventana" subtitle="Referencia exacta y estimación"><ExecutiveLine data={series} x="event" lines={['exact', 'estimate']} formatter={integer} /></Card><div className="insight"><span>Decisión ejecutiva</span><p>DGIM conserva una estimación compacta para una ventana de eventos. La ventana representa cantidad de procesos, no días calendario.</p></div><Technical title="Rejilla DGIM"><DataTable rows={data.dgim} /></Technical></>
}
