import { useMemo, useState } from 'react'
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from 'recharts'
import { api, decimal, integer, percent } from '../../api'
import { Card, DataTable, ErrorState, Filters, FindingModule, Loading, Range, Select, Stat } from '../../components'
import { useAsync } from '../../hooks'

const HASHES = [16, 32, 64, 128, 256]

// Probabilidad de que un par con Jaccard s sea candidato con b bandas de r filas.
const candidate = (s, b, r) => 1 - (1 - s ** r) ** b

export function MinHashLab({ s }) {
  const [index, setIndex] = useState(2)
  const [similarity, setSimilarity] = useState(0.6)
  const k = HASHES[index]
  const row = s.minhash_error.find(r => r.hashes === k)
  const points = s.minhash_scatter.filter(r => r.hashes === k)
  const theory = HASHES.map(h => ({ hashes: h, teórico: Math.sqrt(similarity * (1 - similarity) / h),
    observado: s.minhash_error.find(r => r.hashes === h)?.rmse_similar }))
  return <FindingModule id="lab-minhash" part="Parte II · MinHash" question="¿Cuánto se equivoca MinHash al estimar Jaccard, según el número de funciones hash?"
    pipeline={[['Muestra por bloques', 'Spark', `${integer(s.methodology.sample.size)} procesos de ${s.methodology.sample.buyers} entidades`], ['Jaccard exacto de todos los pares', 'local', 'X·Xᵀ disperso'], ['Firmas de 256 hashes', 'local', 'prefijos = k menores'], ['Ĵ = componentes iguales / k', 'local']]}>
    <div className="lab">
      <Filters>
        <label><span>Funciones hash (k = {k})</span></label>
        <div className="range-steps">{HASHES.map((h, i) => <button key={h} className={i === index ? 'active' : ''} onClick={() => setIndex(i)}>{h}</button>)}</div>
        <Range label="Jaccard de referencia" value={similarity} onChange={setSimilarity} min={0.1} max={0.9} step={0.1} />
        <p className="formula">error estándar = √(J(1−J)/k) = {decimal(Math.sqrt(similarity * (1 - similarity) / k), 3)}</p>
      </Filters>
      <div>
        {row && <div className="stat-grid three">
          <Stat label="RMSE en pares similares" value={decimal(row.rmse_similar, 3)} note={`${integer(row.similar_pairs)} pares con J ≥ 0,2`} />
          <Stat label="RMSE teórico" value={decimal(row.rmse_theoretical_similar, 3)} tone="green" />
          <Stat label="Sesgo" value={decimal(row.bias, 4)} note={`sobre ${integer(row.pairs)} pares`} tone="orange" />
        </div>}
        <div className="two-col">
          <Card title="Jaccard estimado frente a exacto" subtitle={`k = ${k}; la diagonal sería estimación perfecta`}>
            <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
              <ScatterChart margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" dataKey="exact" name="Exacto" domain={[0, 1]} /><YAxis type="number" dataKey="estimate" name="MinHash" domain={[0, 1]} />
                <ZAxis range={[14, 14]} /><Tooltip formatter={v => decimal(v, 3)} /><Scatter data={points} fill="#167c80" fillOpacity={0.45} />
              </ScatterChart></ResponsiveContainer></div>
          </Card>
          <Card title="Error según k" subtitle={`Observado (pares similares) frente a teórico con J = ${similarity}`}>
            <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
              <LineChart data={theory} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hashes" scale="log" domain={[16, 256]} type="number" ticks={HASHES} />
                <YAxis /><Tooltip formatter={v => decimal(v, 3)} /><Legend />
                <Line dataKey="teórico" stroke="#de7c45" strokeWidth={2.5} /><Line dataKey="observado" stroke="#167c80" strokeWidth={2.5} />
              </LineChart></ResponsiveContainer></div>
          </Card>
        </div>
      </div>
    </div>
  </FindingModule>
}

function PairBrowser({ config }) {
  const [status, setStatus] = useState('FP')
  const result = useAsync(() => api('/api/similarity/pairs', { config, status }), [config, status])
  return <Card title="Pares de la muestra según el resultado de LSH" subtitle="TP: candidato y similar · FP: candidato no similar · FN: similar no recuperado">
    <Filters><Select label="Ver" value={status} onChange={setStatus} options={['TP', 'FP', 'FN']} all={false} /></Filters>
    {result.error ? <ErrorState error={result.error} /> : result.loading ? <Loading /> :
      <div className="pair-list">{result.data.pairs.slice(0, 8).map(p => <details key={`${p.left_ocid}-${p.right_ocid}`}>
        <summary><span>{p.same_buyer ? 'Misma entidad' : 'Entidades distintas'}</span><b>Jaccard {decimal(p.jaccard, 3)}</b></summary>
        <div className="compare"><p>{p.left_text}</p><p>{p.right_text}</p></div></details>)}
        {!result.data.pairs.length && <p className="empty">No hay pares en este estado.</p>}</div>}
  </Card>
}

export function LshLab({ s }) {
  const [bands, setBands] = useState(16)
  const [rows, setRows] = useState(8)
  const [threshold, setThreshold] = useState(0.7)
  const curve = useMemo(() => Array.from({ length: 51 }, (_, i) => {
    const sim = i / 50
    const observed = s.s_curve.find(r => r.bands === bands && r.rows === rows && Math.abs(r.similarity - sim) < 0.011)
    return { similarity: sim, teórica: candidate(sim, bands, rows), observada: observed?.observed }
  }), [bands, rows, s.s_curve])
  const config = s.grid.find(r => r.bands === bands && r.rows === rows && r.threshold === threshold)
  return <FindingModule id="lab-lsh" part="Parte II · LSH" question="¿Cómo deciden las bandas y filas qué pares se comparan, y cuántos falsos positivos y negativos dejan?"
    pipeline={[['Firmas MinHash', 'local'], ['b bandas × r filas', 'Spark', 'RDD: banda → documentos'], ['Candidatos', 'Spark'], ['TP/FP/FN contra Jaccard exacto', 'local']]}>
    <div className="lab">
      <Filters>
        <Range label="Bandas (b)" value={bands} onChange={setBands} min={2} max={64} step={2} />
        <Range label="Filas por banda (r)" value={rows} onChange={setRows} min={1} max={10} step={1} />
        <Range label="Umbral de similitud" value={threshold} onChange={setThreshold} min={0.5} max={0.8} step={0.1} />
        <p className="formula">k = b·r = {bands * rows} · umbral efectivo ≈ (1/b)^(1/r) = {decimal((1 / bands) ** (1 / rows), 2)}</p>
        <p className="hint">Los puntos observados y las métricas aparecen cuando (b, r) coincide con una configuración medida: k ∈ {'{32, 64, 128}'}, r ∈ {'{2, 4, 8}'}.</p>
      </Filters>
      <div>
        {config ? <div className="stat-grid four">
          <Stat label="Precisión" value={percent(config.precision)} note={`${integer(config.fp)} falsos positivos`} />
          <Stat label="Recall" value={percent(config.recall)} note={`${integer(config.fn)} falsos negativos`} tone="green" />
          <Stat label="F1" value={decimal(config.f1, 3)} tone="orange" />
          <Stat label="Candidatos" value={integer(config.candidates)} note={`de ${integer(config.all_pairs)} pares`} tone="navy" />
        </div> : <div className="callout positive"><strong>Curva teórica</strong><p>Esta combinación no se midió; la curva se recalcula en vivo con 1 − (1 − s^r)^b.</p></div>}
        <Card title="Curva S: probabilidad de ser candidato" subtitle="Teórica en vivo y observada en la muestra">
          <div style={{ width: '100%', height: 300 }}><ResponsiveContainer>
            <LineChart data={curve} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="similarity" type="number" domain={[0, 1]} />
              <YAxis domain={[0, 1]} tickFormatter={v => percent(v, 0)} /><Tooltip formatter={v => percent(v)} /><Legend />
              <Line dataKey="teórica" stroke="#de7c45" dot={false} strokeWidth={2.5} /><Line dataKey="observada" stroke="#167c80" strokeWidth={0} dot={{ r: 5 }} connectNulls={false} />
            </LineChart></ResponsiveContainer></div>
        </Card>
      </div>
    </div>
    {config && <PairBrowser config={config.config} />}
    <DataTable rows={s.grid.filter(r => r.threshold === threshold).map(r => ({ k: r.hashes, b: r.bands, r: r.rows, 'umbral efectivo': decimal(r.effective_threshold), precisión: percent(r.precision), recall: percent(r.recall), F1: decimal(r.f1, 3), candidatos: r.candidates, 'pares verdaderos': r.truth_pairs, 'tiempo LSH (s)': decimal(r.lsh_seconds) }))} />
  </FindingModule>
}

export default function SimilarityLab() {
  const result = useAsync(() => api('/api/similarity'), [])
  if (result.error) return <ErrorState error={result.error} />
  if (result.loading) return <Loading />
  return <><MinHashLab s={result.data} /><LshLab s={result.data} /></>
}
