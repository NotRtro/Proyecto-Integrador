import { useMemo, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api, bytes, decimal, integer, percent } from '../../api'
import { Card, DataTable, ErrorState, Filters, FindingModule, Loading, Range, Select, Stat } from '../../components'
import { useAsync } from '../../hooks'

const bloomFpr = (m, k, n) => (1 - Math.exp(-k * n / m)) ** k

function BloomLab({ d }) {
  const n = d.live.bloom.inserted
  const [logBits, setLogBits] = useState(19)
  const [hashes, setHashes] = useState(5)
  const bits = 2 ** logBits
  const optimal = Math.max(1, Math.round(bits / n * Math.LN2))
  const curve = useMemo(() => Array.from({ length: 13 }, (_, i) => {
    const lb = 14 + i
    const observed = d.bloom.find(r => r.bits === 2 ** lb && r.hashes === hashes)
    // Filtro lleno en ambos lados: la curva usa n final y el punto, las sondas medidas al final del flujo.
    return { bits: `2^${lb}`, teórica: bloomFpr(2 ** lb, hashes, n), observada: observed?.final_fpr ?? observed?.observed_fpr }
  }), [hashes, n, d.bloom])
  const exact = d.bloom[0]?.exact_bytes
  return <FindingModule id="lab-bloom" part="Parte IV · Bloom Filter" question="¿Cuánta memoria hace falta para recordar qué proveedores ya ganaron con cada entidad, y cuántos falsos positivos cuesta?"
    pipeline={[['Flujo de adjudicaciones', 'Spark', 'orden por fecha'], ['Clave (entidad, proveedor)', 'local'], ['Consultar y luego insertar', 'local', 'positivo para un par nuevo = falso positivo'], ['Conjunto exacto paralelo', 'local']]}>
    <div className="lab">
      <Filters>
        <Range label="Bits (log₂ m)" value={logBits} onChange={setLogBits} min={14} max={26} step={1} />
        <Range label="Funciones hash (k)" value={hashes} onChange={setHashes} min={1} max={12} step={1} />
        <p className="formula">FPR = (1 − e^(−k·n/m))^k = {percent(bloomFpr(bits, hashes, n), 3)}</p>
        <p className="hint">n = {integer(n)} pares distintos insertados · k óptimo para este m: {optimal}</p>
      </Filters>
      <div>
        <div className="stat-grid three">
          <Stat label="Memoria del filtro" value={bytes(bits / 8)} note={`frente a ${bytes(exact)} del conjunto exacto`} />
          <Stat label="Falsos positivos (teórico)" value={percent(bloomFpr(bits, hashes, n), 2)} tone="orange" />
          <Stat label="Falsos negativos" value={integer(d.bloom.reduce((s, r) => s + r.fn, 0))} note="siempre 0 (verificado)" tone="green" />
        </div>
        <Card title="Tasa de falsos positivos según el tamaño del filtro" subtitle={`k = ${hashes}; curva teórica con el filtro lleno y puntos medidos con sondas al final del flujo`}>
          <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
            <LineChart data={curve} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="bits" /><YAxis tickFormatter={v => percent(v, 0)} />
              <Tooltip formatter={v => percent(v, 3)} /><Legend /><Line dataKey="teórica" stroke="#de7c45" strokeWidth={2.5} dot={false} />
              <Line dataKey="observada" stroke="#167c80" strokeWidth={0} dot={{ r: 5 }} />
            </LineChart></ResponsiveContainer></div>
        </Card>
      </div>
    </div>
    <DataTable rows={d.bloom.map(r => ({ bits: r.bits, k: r.hashes, 'k óptimo': r.optimal_hashes, memoria: bytes(r.bytes), 'FP en el flujo': r.fp, 'FPR flujo (observado)': percent(r.observed_fpr, 3), 'FPR flujo (teórico)': r.expected_stream_fpr == null ? '—' : percent(r.expected_stream_fpr, 3), 'FPR final (sondas)': r.final_fpr == null ? '—' : percent(r.final_fpr, 3), 'FPR final (teórico)': percent(r.theoretical_fpr, 3), 'µs por operación': decimal(r.operation_us, 1) }))} />
    <p className="hint">El flujo consulta el filtro mientras se llena: su FPR se compara con la fórmula evaluada con la ocupación de cada consulta. El FPR final se mide con 20 000 claves nunca insertadas y se compara con la fórmula con los n pares del final.</p>
  </FindingModule>
}

function CmsLab({ d }) {
  const [domain, setDomain] = useState('proveedor')
  const [logWidth, setLogWidth] = useState(11)
  const [depth, setDepth] = useState(3)
  const width = 2 ** logWidth
  const live = d.live.cms?.[domain] || { events: 0, distinct: 0 }
  const epsilon = Math.E / width
  const measured = d.cms.filter(r => r.domain === domain)
  const exactRow = measured.find(r => r.width === width && r.depth === depth)
  const nearest = exactRow || [...measured].sort((a, b) => Math.abs(Math.log2(a.width) - logWidth) - Math.abs(Math.log2(b.width) - logWidth) || Math.abs(a.depth - depth) - Math.abs(b.depth - depth))[0]
  const top = d.cms_top.filter(r => r.domain === domain && r.width === nearest?.width && r.depth === nearest?.depth).map(r => ({ ...r, key: String(r.key).slice(-12) }))
  return <FindingModule id="lab-cms" part="Parte IV · Count-Min Sketch" question="¿Con qué error se puede contar cuántas adjudicaciones lleva cada proveedor (o cuántas veces se pidió cada código UNSPSC) usando memoria fija?"
    pipeline={[['Flujo ordenado', 'Spark', 'adjudicaciones o ítems'], ['d filas × w contadores', 'local'], ['Estimado = mínimo de d celdas', 'local'], ['Conteo exacto paralelo', 'local']]}>
    <div className="lab">
      <Filters>
        <Select label="Qué se cuenta" value={domain} onChange={setDomain} options={['proveedor', 'unspsc']} all={false} format={v => v === 'proveedor' ? 'Proveedores' : 'Códigos UNSPSC'} />
        <Range label="Ancho (log₂ w)" value={logWidth} onChange={setLogWidth} min={7} max={16} step={1} />
        <Range label="Profundidad (d)" value={depth} onChange={setDepth} min={1} max={8} step={1} />
        <p className="formula">ε = e/w = {decimal(epsilon, 5)} · δ = e^(−d) = {decimal(Math.exp(-depth), 4)}</p>
        <p className="hint">Con probabilidad ≥ {percent(1 - Math.exp(-depth))}, el error ≤ ε·N = {integer(epsilon * live.events)} sobre N = {integer(live.events)} eventos y {integer(live.distinct)} claves distintas.</p>
      </Filters>
      <div>
        {nearest && <div className="stat-grid four">
          <Stat label="Memoria" value={bytes(width * depth * 8)} note={`exacto: ${bytes(nearest.exact_bytes)}`} />
          <Stat label="Error relativo (100 más frecuentes)" value={percent(nearest.heavy_mean_relative_error)} note={`medido con ${nearest.width} × ${nearest.depth}`} tone="green" />
          <Stat label="Error relativo (cola)" value={percent(nearest.tail_mean_relative_error)} note="mitad menos frecuente" tone="orange" />
          <Stat label="Dentro de la cota" value={percent(nearest.within_bound)} note={`garantía ${percent(nearest.guaranteed)}`} tone="navy" />
        </div>}
        <Card title="Los 30 más frecuentes: exacto frente a estimado" subtitle={nearest ? `Configuración medida más cercana: ${nearest.width} × ${nearest.depth}` : ''}>
          <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
            <BarChart data={top} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" vertical={false} /><XAxis dataKey="key" tick={{ fontSize: 9 }} interval={2} /><YAxis tickFormatter={integer} />
              <Tooltip formatter={integer} /><Legend /><Bar dataKey="exact" name="Exacto" fill="#384b72" /><Bar dataKey="estimate" name="Estimado" fill="#de7c45" />
            </BarChart></ResponsiveContainer></div>
        </Card>
      </div>
    </div>
    <DataTable rows={measured.map(r => ({ w: r.width, d: r.depth, memoria: bytes(r.bytes), 'error medio': decimal(r.mean_abs_error, 1), 'error máximo': r.max_abs_error, 'cota ε·N': integer(r.error_bound), 'dentro de cota': percent(r.within_bound), 'exactos': percent(r.exact_share), 'µs/actualización': decimal(r.update_us, 1) }))} />
  </FindingModule>
}

function DgimLab({ d }) {
  const [key, setKey] = useState(`${d.dgim[0].signal}|${d.dgim[0].unit}|${d.dgim[0].window}`)
  const [signal, unit, size] = key.split('|')
  const row = d.dgim.find(r => r.signal === signal && r.unit === unit && String(r.window) === size)
  const series = d.dgim_series.filter(r => r.signal === signal && r.unit === unit && String(r.window) === size)
  const label = { postor_unico: 'un solo postor', alto_monto: 'monto ≥ S/ 1 millón' }
  return <FindingModule id="lab-dgim" part="Parte IV · DGIM" question="¿Qué error se comete al contar eventos recientes con memoria logarítmica?"
    pipeline={[['Flujo de adjudicaciones', 'Spark'], ['Bit por evento', 'local', 'un postor o alto monto'], ['Buckets de tamaño 2^i', 'local', 'a lo sumo dos por tamaño'], ['Ventana exacta paralela', 'local']]}>
    <Filters><Select label="Señal y ventana" value={key} onChange={setKey} all={false}
      options={d.dgim.map(r => `${r.signal}|${r.unit}|${r.window}`)} format={v => { const [s, u, w] = v.split('|'); return `${label[s]} · últimos ${integer(w)} ${u}` }} /></Filters>
    {row && <div className="stat-grid four">
      <Stat label="Exacto" value={integer(row.exact)} /><Stat label="DGIM" value={decimal(row.estimate, 1)} tone="green" />
      <Stat label="Error relativo" value={row.relative_error == null ? '—' : percent(row.relative_error)} note="cota: 50 %" tone="orange" />
      <Stat label="Memoria" value={`${row.buckets} buckets · ${bytes(row.bytes)}`} note={`ventana exacta: ${bytes(row.exact_bytes)}`} tone="navy" />
    </div>}
    <Card title="Conteo en la ventana a lo largo del flujo" subtitle="Exacto frente a estimado">
      <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
        <LineChart data={series} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="event" tickFormatter={integer} /><YAxis tickFormatter={integer} />
          <Tooltip /><Legend /><Line dataKey="exact" name="Exacto" stroke="#384b72" dot={false} /><Line dataKey="estimate" name="DGIM" stroke="#de7c45" dot={false} />
        </LineChart></ResponsiveContainer></div>
    </Card>
    <DataTable rows={d.dgim.map(r => ({ señal: label[r.signal], ventana: `${integer(r.window)} ${r.unit}`, exacto: r.exact, DGIM: r.estimate, 'error relativo': r.relative_error == null ? '—' : percent(r.relative_error), buckets: r.buckets, memoria: bytes(r.bytes), 'memoria exacta': bytes(r.exact_bytes) }))} />
  </FindingModule>
}

export default function StreamLab() {
  const result = useAsync(() => api('/api/streaming'), [])
  if (result.error) return <ErrorState error={result.error} />
  if (result.loading) return <Loading />
  return <><BloomLab d={result.data} /><CmsLab d={result.data} /><DgimLab d={result.data} /></>
}
