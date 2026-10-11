import { useRef, useState } from 'react'
import { api, decimal, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, FindingModule, Loading, SectionHeader, Select, Stat } from '../components'
import { useAsync, useMeta } from '../hooks'

const DIMENSIONS = { category: 'Categoría', modality: 'Modalidad', amount_band: 'Banda de monto', department: 'Departamento', buyer_level: 'Nivel de gobierno', year: 'Año' }
const CATEGORIES = { goods: 'Bienes', services: 'Servicios', works: 'Obras' }
const TONES = ['blue', 'green', 'orange', 'navy']

export default function Signals() {
  const meta = useMeta()
  const [dimension, setDimension] = useState('category')
  const [signal, setSignal] = useState('single_bidder')
  const [year, setYear] = useState('Todos')
  const [order, setOrder] = useState('any')
  const result = useAsync(() => api('/api/integrity', { dimension }), [dimension])
  // Al cambiar de dimensión se conserva la respuesta anterior hasta que llega la nueva.
  const last = useRef(null)
  if (result.data) last.current = result.data
  if (result.error || meta.error) return <ErrorState error={result.error || meta.error} />
  const d = last.current
  if (!d || meta.loading) return <Loading />
  const lsh = d.methodology.repeated_detection
  const signals = d.methodology.signals
  const labels = Object.fromEntries(Object.entries(signals).map(([key, value]) => [key, value.label]))
  const orders = { any: 'Cualquier señal', ...labels }
  const cards = d.summary.filter(r => String(r.year) === year)
  const breakdown = d.by_dimension.filter(r => r.signal === signal && r.evaluable >= 200)
    .map(r => ({ ...r, value: CATEGORIES[r.value] || String(r.value) })).sort((a, b) => b.rate - a.rate)
  const none = d.counts.find(r => r.signal_count === 0)
  const several = d.counts.filter(r => r.signal_count >= 2).reduce((sum, r) => sum + r.processes, 0)
  const column = order === 'any' ? 'with_signal_share' : `${order}_rate`
  return <>
    <SectionHeader eyebrow="Q4 · Señales de riesgo" title="Señales para revisar, no acusaciones"
      description="Cuatro indicadores marcan procesos que conviene mirar de cerca. Combinan lo que ya miden las otras preguntas: competencia, modalidad, dependencia de un proveedor y requerimientos repetidos. Cada uno se reporta con su propio denominador."
      question="¿Dónde se concentran las señales de riesgo y tienden a aparecer juntas en un mismo proceso?" />
    <div className="callout"><strong>Qué es una señal</strong><p>{d.methodology.principle} Estos datos no contienen sanciones, denuncias ni resultados de auditoría, así que no permiten afirmar corrupción.</p></div>
    <Filters><Select label="Año de las tarjetas" value={year} onChange={setYear} options={meta.data.years.map(String)} /></Filters>
    <div className="stat-grid four">{cards.map((r, i) => <Stat key={r.signal} label={r.label} value={percent(r.rate, 2)} note={`${integer(r.flagged)} de ${integer(r.evaluable)} evaluables`} tone={TONES[i]} />)}</div>

    <p className="scope">El año solo cambia estas tarjetas; los gráficos y tablas usan todo el periodo. «Compra repetida» cuenta lo que detectó LSH (Jaccard verificado de {decimal(lsh.threshold, 1)} o más): la búsqueda no es exhaustiva y no expande {integer(lsh.oversized_buckets)} grupos de más de {integer(lsh.max_bucket)} documentos, así que es repetición detectada, no total.</p>

    <FindingModule id="q4-senales" part="Partes I y II · señales por proceso" question="¿Dónde se concentra cada señal?"
      pipeline={[['processes, suppliers y pares repetidos', 'Spark'], ['Una fila por proceso', 'Spark', 'evaluable y marcada, por señal'], ['groupBy dimensión', 'Spark', 'marcados / evaluables']]}>
      <Filters>
        <Select label="Señal" value={signal} onChange={setSignal} options={Object.keys(labels)} all={false} format={v => labels[v]} />
        <Select label="Comparar por" value={dimension} onChange={setDimension} options={d.dimensions} all={false} format={v => DIMENSIONS[v] || v} />
      </Filters>
      <Card title={labels[signal]} subtitle="Proporción de procesos marcados entre los evaluables; grupos con al menos 200 evaluables">
        <ExecutiveBar data={breakdown.slice(0, 15)} category="value" value="rate" formatter={v => percent(v)} height={Math.max(220, 30 * Math.min(15, breakdown.length) + 60)} />
      </Card>
      <DataTable rows={Object.values(signals).map(s => ({ señal: s.label, 'qué marca': s.definition, 'dónde se evalúa': s.evaluable, 'explicaciones legítimas': s.caveat }))} />
      <DataTable rows={d.summary.filter(r => String(r.year) === 'Todos').map(r => ({ señal: r.label, evaluables: r.evaluable, marcados: r.flagged, tasa: percent(r.rate, 2), 'no evaluables': percent(r.not_evaluable_share) }))} />
    </FindingModule>

    <FindingModule id="q4-coincidencia" part="Parte I · coincidencia entre señales" question="¿Las señales aparecen juntas en el mismo proceso?"
      pipeline={[['Pares de señales', 'Spark', 'solo procesos evaluables en ambas'], ['Observado frente a esperado', 'Spark', 'lift = 1 si fueran independientes']]}>
      <div className="stat-grid three">
        <Stat label="Sin ninguna señal" value={percent(none?.share)} note={`${integer(none?.processes)} procesos`} />
        <Stat label="Con dos o más señales" value={integer(several)} note="procesos a priorizar" tone="orange" />
        <Stat label="Procesos analizados" value={integer(d.methodology.processes)} note={meta.data.period} tone="green" />
      </div>
      <Card title="Coincidencia entre pares de señales" subtitle="Un lift mayor que 1 indica que coinciden más de lo esperable por azar">
        <DataTable rows={d.cooccurrence.map(r => ({ 'señal A': r.label_a, 'señal B': r.label_b, 'evaluables en ambas': r.evaluable_both, coinciden: r.lift == null ? 'no comparables' : r.joint, 'esperado por azar': r.lift == null ? 'no comparables' : decimal(r.expected_joint, 1), lift: r.lift == null ? 'no comparables' : decimal(r.lift) }))} />
      </Card>
    </FindingModule>

    <FindingModule id="q4-entidades" part="Parte I · entidades públicas" question="¿Qué entidades públicas acumulan mayor proporción de procesos marcados?"
      pipeline={[['Señales por proceso', 'Spark'], ['groupBy entidad', 'Spark', 'entidades con 50 procesos o más'], ['Las 15 con mayor proporción', 'local']]}>
      <Filters><Select label="Ordenar por" value={order} onChange={setOrder} options={Object.keys(orders)} all={false} format={v => orders[v]} /></Filters>
      <DataTable rows={(d.buyers[order] || []).map(r => ({ entidad: r.buyer_name, departamento: r.department, nivel: r.buyer_level, procesos: r.processes, ...(order === 'any' ? {} : { [orders[order].toLowerCase()]: percent(r[column]) }), 'con alguna señal': percent(r.with_signal_share) }))} />
    </FindingModule>
  </>
}
