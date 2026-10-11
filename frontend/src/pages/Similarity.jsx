import { useMemo, useState } from 'react'
import { api, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, Loading, SectionHeader, Select, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Similarity() {
  const base = useAsync(() => api('/api/similarity'), [])
  const [config, setConfig] = useState('')
  const [status, setStatus] = useState('TP')
  const active = config || base.data?.selected?.[0]?.config || ''
  const result = useAsync(() => active ? api('/api/similarity', { config: active, status }) : Promise.resolve(null), [active, status])
  const configs = useMemo(() => base.data?.grid?.map(r => r.config) || [], [base.data])
  // El error se revisa primero: si la carga base falla, active queda vacío y la página quedaría cargando.
  if (base.error || result.error) return <ErrorState error={base.error || result.error} />
  if (base.loading || !active || result.loading || !result.data) return <Loading />
  const row = result.data.selected[0] || base.data.selected[0]
  const chart = base.data.grid.filter(r => r.threshold === row.threshold).map(r => ({ ...r, label: `${r.hashes}h · ${r.bands}b` })).sort((a, b) => b.f1 - a.f1).slice(0, 12)
  return <>
    <SectionHeader eyebrow="Pregunta 2" title="Descripciones similares, con errores visibles"
      description="LSH reduce el espacio de búsqueda y Jaccard exacto confirma qué candidatos son realmente similares."
      question="¿Qué procesos presentan textos similares y qué precisión obtiene MinHash LSH?" />
    <Filters><Select label="Configuración" value={active} onChange={setConfig} options={configs} all={false} /><Select label="Ver pares" value={status} onChange={setStatus} options={['TP', 'FP', 'FN']} all={false} /></Filters>
    <div className="stat-grid four">
      <Stat label="Precisión" value={percent(row.precision)} note="Candidatos correctos" />
      <Stat label="Recall" value={percent(row.recall)} note="Similares recuperados" tone="green" />
      <Stat label="F1" value={Number(row.f1).toFixed(3)} note="Balance de métricas" tone="orange" />
      <Stat label="Candidatos" value={row.candidates.toLocaleString('es-PE')} note={`${row.truth_pairs} pares verdaderos`} tone="navy" />
    </div>
    <div className="two-col">
      <Card title="Comparación de configuraciones" subtitle={`F1 para umbral Jaccard ${row.threshold}`}><ExecutiveBar data={chart} category="label" value="f1" formatter={percent} /></Card>
      <Card title="Lectura ejecutiva" subtitle="Qué significa esta configuración"><div className="narrative"><p>Con <strong>{row.hashes} funciones hash</strong> y {row.bands} bandas, el método recupera {percent(row.recall)} de los pares que la referencia exacta considera similares.</p><p>La precisión de {percent(row.precision)} indica qué proporción de candidatos evita una revisión innecesaria.</p><p>El umbral actual es <strong>{row.threshold}</strong>. Cambiarlo modifica la definición de similitud y, por tanto, los errores.</p></div></Card>
    </div>
    <Card title={status === 'TP' ? 'Coincidencias verificadas' : status === 'FP' ? 'Falsos positivos para revisar' : 'Similares que LSH no recuperó'} subtitle={`${result.data.total_pairs} pares bajo la selección`}>
      <div className="pair-list">{result.data.pairs.slice(0, 8).map(pair => <details key={`${pair.left_ocid}-${pair.right_ocid}`}><summary><span>{pair.left_ocid}</span><b>Jaccard {Number(pair.jaccard).toFixed(3)}</b></summary><div className="compare"><p>{pair.left_text}</p><p>{pair.right_text}</p></div></details>)}</div>
    </Card>
    <div className="callout"><strong>Límite interpretativo</strong><p>Dos compras pueden compartir vocabulario, ubicación o proyecto y contratar objetos distintos. La similitud textual sirve para priorizar revisión; no acredita duplicidad ni irregularidad.</p></div>
    <Technical title="Detalle técnico: rejilla LSH y casos interpretados">
      <h4>Parámetros y métricas</h4><DataTable rows={base.data.grid} />
      <h4>Casos interpretados</h4>{result.data.review.cases?.map((item, i) => <div className="review" key={i}><p>{item.interpretation}</p><small>{item.limitation}</small></div>)}
      <h4>Metodología reproducible</h4><pre>{JSON.stringify(result.data.methodology, null, 2)}</pre>
    </Technical>
  </>
}
