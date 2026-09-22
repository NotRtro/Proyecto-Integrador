import { useEffect, useState } from 'react'
import { api, ms, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, Loading, SectionHeader, Select, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Ann() {
  const initial = useAsync(() => api('/api/ann'), [])
  const [query, setQuery] = useState(''); const [config, setConfig] = useState('exact'); const [k, setK] = useState('5')
  useEffect(() => { if (!query && initial.data?.query?.ocid) setQuery(initial.data.query.ocid) }, [initial.data, query])
  const result = useAsync(() => query ? api('/api/ann', { query, config, k }) : Promise.resolve(null), [query, config, k])
  if (initial.loading || !query || result.loading || !result.data) return <Loading />
  if (initial.error || result.error) return <ErrorState error={initial.error || result.error} />
  const d = result.data; const selected = d.grid.find(r => r.config === config)
  const configs = ['exact', ...initial.data.grid.map(r => r.config)]
  const speed = [...initial.data.grid].sort((a, b) => b.speedup - a.speedup).slice(0, 8)
  return <>
    <SectionHeader eyebrow="Pregunta 3" title="Búsqueda rápida de procesos relacionados"
      description="Consulta vecinos semánticos precalculados y compara la referencia exacta con configuraciones HNSW."
      question="¿Qué aceleración consigue HNSW y cuánto recall conserva frente al coseno exacto?" />
    <Filters><Select label="Consulta" value={query} onChange={setQuery} options={initial.data.queries.map(q => q.ocid)} all={false} /><Select label="Algoritmo" value={config} onChange={setConfig} options={configs} all={false} /><Select label="Vecinos" value={k} onChange={setK} options={['5', '10']} all={false} /></Filters>
    <Card title="Proceso consultado" subtitle={d.query.ocid}><p className="query-text">{d.query.text}</p></Card>
    {selected ? <div className="stat-grid four"><Stat label="Recall@10" value={percent(selected.recall_at_10)} /><Stat label="Aceleración" value={`${Number(selected.speedup).toFixed(2)}×`} tone="green" /><Stat label="Latencia mediana" value={ms(selected.median_ms)} tone="orange" /><Stat label="Índice" value={`${(selected.index_bytes / 1048576).toFixed(1)} MiB`} tone="navy" /></div> : <div className="callout positive"><strong>Referencia exacta</strong><p>Esta consulta usa producto punto sobre todos los vectores TF-IDF normalizados y sirve como base para medir recall.</p></div>}
    <Card title="Vecinos recuperados" subtitle="Ordenados por similitud coseno"><div className="neighbor-list">{d.neighbors.map(n => <div key={n.neighbor_ocid}><span>{n.rank}</span><p><strong>{n.neighbor_ocid}</strong>{n.text}</p><b>{Number(n.similarity).toFixed(3)}</b></div>)}</div></Card>
    <Card title="Aceleración por configuración" subtitle="Más alto significa menor tiempo medio que la búsqueda exacta"><ExecutiveBar data={speed} category="config" value="speedup" formatter={v => `${Number(v).toFixed(1)}×`} /></Card>
    <div className="callout"><strong>Cómo leer el resultado</strong><p>El recall compara identificadores recuperados. Descripciones idénticas pueden producir empates y cambiar los identificadores sin cambiar la calidad semántica. Una aceleración menor que 1× significa que, para este corpus, el cálculo exacto fue más rápido.</p></div>
    <Technical title="Detalle técnico: HNSW, latencia e índice">
      <DataTable rows={initial.data.grid} />
      <div className="detail-grid"><div><b>Representación</b><p>TF-IDF, unigramas y bigramas, máximo 4096 dimensiones, norma L2.</p></div><div><b>Corpus</b><p>{initial.data.grid[0].corpus_size} procesos y {initial.data.grid[0].queries} consultas reproducibles.</p></div><div><b>IVF</b><p>{d.ivf.reason}</p></div></div>
    </Technical>
  </>
}
