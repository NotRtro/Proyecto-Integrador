import { useState } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api, decimal, integer, ms, percent } from '../api'
import { Card, DataTable, ErrorState, Filters, FindingModule, Loading, SectionHeader, Select, Stat } from '../components'
import { useAsync } from '../hooks'
import reviewed from '../content/similarity_cases.json'
import LshGroups from './LshGroups'

const KINDS = ['misma entidad, <= 30 días', 'misma entidad, > 30 días', 'entidades distintas']

function NeighborBrowser({ ann }) {
  const [query, setQuery] = useState(ann.queries[0]?.ocid)
  const [config, setConfig] = useState('exact')
  const result = useAsync(() => api('/api/ann/neighbors', { query, config }), [query, config])
  const current = ann.queries.find(q => q.ocid === query)
  const grid = [...ann.hnsw, ...ann.ivf].find(r => r.config === config)
  return <>
    <Filters>
      <Select label="Proceso consultado" value={query} onChange={setQuery} all={false} options={ann.queries.map(q => q.ocid)}
        format={v => (ann.queries.find(q => q.ocid === v)?.text || v).slice(0, 70)} />
      <Select label="Búsqueda" value={config} onChange={setConfig} all={false} options={ann.configs}
        format={v => v === 'exact' ? 'Exacta (fuerza bruta)' : v.startsWith('IVF') ? `IVF · ${v}` : `HNSW · ${v}`} />
    </Filters>
    <Card title="Proceso consultado" subtitle={query}><p className="query-text">{current?.text}</p></Card>
    {grid && <div className="stat-grid three">
      <Stat label="Recall@10" value={percent(grid.recall_at_10)} note={`IC 95 % ${percent(grid.recall_at_10_ci_low)} – ${percent(grid.recall_at_10_ci_high)}`} />
      <Stat label="Latencia media" value={ms(grid.mean_ms)} note={`exacta: ${ms(grid.exact_mean_ms)}`} tone="green" />
      <Stat label="Aceleración" value={`${decimal(grid.speedup, 0)}×`} note="frente a la búsqueda exacta" tone="orange" />
    </div>}
    {result.error ? <ErrorState error={result.error} /> : result.loading ? <Loading /> :
      <Card title="Diez vecinos más parecidos" subtitle="Similitud coseno sobre TF-IDF + SVD"><div className="neighbor-list">{result.data.neighbors.map(n =>
        <div key={n.neighbor_ocid}><span>{n.rank}</span><p><strong>{n.neighbor_ocid}</strong>{n.text}</p><b>{decimal(n.similarity, 3)}</b></div>)}</div></Card>}
  </>
}

export default function Similarity() {
  const similarity = useAsync(() => api('/api/similarity'), [])
  const ann = useAsync(() => api('/api/ann'), [])
  const [kind, setKind] = useState(KINDS[0])
  const error = similarity.error || ann.error
  if (error) return <ErrorState error={error} />
  if (similarity.loading || ann.loading) return <Loading />
  const s = similarity.data
  const corpus = s.methodology.full_corpus
  const funnel = [
    { paso: 'Pares posibles', pares: corpus.possible_pairs },
    { paso: 'Candidatos LSH', pares: corpus.candidate_pairs },
    { paso: 'Verificados J ≥ 0,7', pares: corpus.similar_pairs },
  ]
  const byKind = Object.values(s.repeated_summary.reduce((acc, r) => {
    acc[r.year] = acc[r.year] || { year: String(r.year) }; acc[r.year][r.kind] = r.processes_involved; return acc
  }, {})).sort((a, b) => a.year.localeCompare(b.year))
  const cases = (s.case_candidates.cases || []).filter(c => c.kind === kind)
  return <>
    <SectionHeader eyebrow="Q3 · Requerimientos similares" title="El mismo requerimiento, otra vez"
      description="Comparar cada proceso con todos los demás exigiría miles de millones de pares. MinHash y LSH encuentran los casi idénticos en minutos; luego se clasifica quién los repite y cuándo."
      question="¿Hay requerimientos casi idénticos convocados repetidamente por una misma entidad en poco tiempo, o por muchas entidades a la vez?" />
    <div className="stat-grid four">
      <Stat label="Procesos con texto" value={integer(corpus.documents)} />
      <Stat label="Pares posibles" value={integer(corpus.possible_pairs)} note="comparación exhaustiva" tone="orange" />
      <Stat label="Candidatos LSH" value={integer(corpus.candidate_pairs)} note={`${percent(corpus.candidate_pairs / corpus.possible_pairs, 4)} de los pares`} tone="green" />
      <Stat label="Casi idénticos verificados" value={integer(corpus.similar_pairs)} note={`Jaccard ≥ ${corpus.threshold}`} tone="navy" />
    </div>

    <FindingModule id="q3-corpus" part="Parte II · LSH en Spark sobre todo el corpus" question="¿Cuánto trabajo evita LSH y cuánto tarda sobre todo el corpus?"
      pipeline={[['Shingles de 2 palabras', 'Spark', 'sin nomenclatura ni stopwords de dominio'], ['Firmas MinHash (k=128)', 'Spark', 'mapPartitions + numpy'], [`${corpus.bands} bandas × ${corpus.rows} filas`, 'Spark', `umbral efectivo ≈ ${decimal(corpus.effective_threshold)}`], ['Pares por bucket', 'Spark'], ['Jaccard exacto de candidatos', 'Spark']]}>
      <Card title="De todos los pares a los casi idénticos" subtitle="Escala logarítmica">
        <div style={{ width: '100%', height: 240 }}><ResponsiveContainer>
          <BarChart data={funnel} layout="vertical" margin={{ left: 30, right: 30 }}><CartesianGrid strokeDasharray="3 3" horizontal={false} />
            <XAxis type="number" scale="log" domain={[1, 'auto']} tickFormatter={integer} /><YAxis dataKey="paso" type="category" width={140} />
            <Tooltip formatter={integer} /><Bar dataKey="pares" fill="#167c80" />
          </BarChart></ResponsiveContainer></div>
      </Card>
      <DataTable rows={[{ firmas: `${decimal(corpus.signatures_seconds, 1)} s`, buckets: `${decimal(corpus.buckets_seconds, 1)} s`, candidatos: `${decimal(corpus.candidates_seconds, 1)} s`, verificación: `${decimal(corpus.verification_seconds, 1)} s`, 'precisión de candidatos': percent(corpus.candidate_precision), 'buckets > 200 (plantillas)': corpus.oversized_buckets }]} />
    </FindingModule>

    <FindingModule id="q3-repetidos" part="Parte II · Q3" question="¿Quién repite el mismo requerimiento: la misma entidad en pocos días, o muchas entidades?"
      pipeline={[['Pares verificados', 'Spark'], ['Contexto de ambos procesos', 'Spark', 'entidad, fecha, nomenclatura'], ['Clasificación', 'Spark', '≤ 30 días, recurrente, entidades distintas; reconvocatorias']]}>
      <Card title="Procesos involucrados en repeticiones" subtitle="Procesos distintos con al menos un casi duplicado, por tipo y año">
        <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
          <BarChart data={byKind} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" vertical={false} /><XAxis dataKey="year" /><YAxis tickFormatter={integer} />
            <Tooltip formatter={integer} /><Legend />{KINDS.map((k, i) => <Bar key={k} dataKey={k} fill={['#de7c45', '#167c80', '#384b72'][i]} />)}
          </BarChart></ResponsiveContainer></div>
      </Card>
      <DataTable rows={s.repeated_summary.map(r => ({ tipo: r.kind, año: r.year, pares: r.pairs, 'procesos involucrados': r.processes_involved, reconvocatorias: r.reconvocation_pairs, 'mismo procedimiento': r.same_procedure_pairs ?? '—', entidades: r.buyers, 'Jaccard medio': decimal(r.mean_jaccard, 3) }))} />
      <div className="two-col">
        <Card title="Entidades que más repiten en ≤ 30 días" subtitle="Sin reconvocatorias ni pares del mismo procedimiento">
          <DataTable rows={s.repeated_top_buyers.map(r => ({ entidad: r.left_buyer_name, nivel: r.left_buyer_level, procesos: r.processes_involved, pares: r.pairs, 'días entre sí': decimal(r.mean_days_apart, 1) }))} limit={12} />
        </Card>
        <Card title="Textos compartidos por cientos de procesos" subtitle="Buckets LSH demasiado grandes para expandir en pares">
          <DataTable rows={s.templates.map(r => ({ procesos: r.processes, entidades: r.buyers, texto: String(r.example_text).slice(0, 120) }))} limit={10} />
        </Card>
      </div>
      <Card title="Casos para revisar en SEACE" subtitle="Pares de mayor monto por tipo; buscar cada proceso por su nomenclatura">
        <Filters><Select label="Tipo" value={kind} onChange={setKind} options={KINDS} all={false} /></Filters>
        <div className="pair-list">{cases.map(c => <details key={`${c.left}-${c.right}`}>
          <summary><span>{c.left_tender_code} · {c.right_tender_code}</span><b>Jaccard {decimal(c.jaccard, 3)} · {c.days_apart} días</b></summary>
          <div className="compare"><p><strong>{c.left_buyer_name}</strong><br />{c.left_text}</p><p><strong>{c.right_buyer_name}</strong><br />{c.right_text}</p></div>
        </details>)}</div>
      </Card>
      <Card title="Casos interpretados" subtitle="Pares revisados en el registro OCDS completo de ambos procesos; la nomenclatura permite ubicarlos en el buscador del SEACE">
        {reviewed.casos.length ? <div className="pair-list">{reviewed.casos.map(c => <details key={`${c.left}-${c.right}`} open={c === reviewed.casos[0]}>
          <summary><span>{c.titulo}</span><b>{c.tipo} · Jaccard {decimal(c.jaccard, 2)}</b></summary>
          <p className="hint">{c.left_tender_code} · {c.right_tender_code} — {c.entidades} · estado: {c.estado}</p>
          <p><strong>Lectura.</strong> {c.interpretacion}</p>
          <p><strong>Límite.</strong> {c.limitacion}</p>
        </details>)}</div> : <p className="empty">Sin casos interpretados.</p>}
      </Card>
    </FindingModule>

    <LshGroups s={s} />

    <FindingModule id="q3-vecinos" part="Parte III · búsqueda de vecinos" question="Dado un proceso, ¿cuáles son los más parecidos, y cuánto se pierde al buscarlos de forma aproximada?"
      pipeline={[['Texto normalizado', 'Spark'], ['TF-IDF (1–2 gramas) + SVD 256', 'local', 'scikit-learn'], ['Índice HNSW o IVF', 'local', 'faiss'], ['Top-10 por coseno', 'local', 'frente a fuerza bruta']]}>
      <NeighborBrowser ann={ann.data} />
    </FindingModule>
  </>
}
