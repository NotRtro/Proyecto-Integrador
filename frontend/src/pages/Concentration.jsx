import { useState } from 'react'
import { api, integer, money, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, ExecutiveLine, Filters, Loading, Range, SectionHeader, Select, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

const labels = { processes: 'Número de procesos', tender_amount_pen: 'Monto convocado', award_amount_pen: 'Monto adjudicado' }

export default function Concentration() {
  const opts = useAsync(() => api('/api/eda/options'), [])
  const [filters, setFilters] = useState({ year: 'Todos', month: 'Todos', department: 'Todos', category: 'Todos', buyer_name: 'Todos', metric: 'award_amount_pen', limit: 10 })
  const results = useAsync(() => api('/api/eda', filters), Object.values(filters))
  const technical = useAsync(() => api('/api/eda/technical'), [])
  const set = (key, value) => setFilters(current => ({ ...current, [key]: value }))
  if (opts.loading || results.loading) return <Loading />
  if (opts.error || results.error) return <ErrorState error={opts.error || results.error} />
  const d = results.data; const formatter = filters.metric === 'processes' ? integer : money
  const monthly = d.monthly.sort((a, b) => a.year - b.year || a.month - b.month).map(row => ({ ...row, period: `${row.year}-${String(row.month).padStart(2, '0')}` }))
  return <>
    <SectionHeader eyebrow="Pregunta 1" title="Concentración y evolución"
      description="Sigue la evolución del mercado público e identifica dónde se concentra el valor bajo una lectura ejecutiva y filtrable."
      question="¿Cómo se distribuye el valor entre entidades, proveedores, categorías y departamentos?" />
    <Filters>
      <Select label="Año" value={filters.year} onChange={v => set('year', v)} options={opts.data.year} />
      <Select label="Mes" value={filters.month} onChange={v => set('month', v)} options={opts.data.month} />
      <Select label="Departamento" value={filters.department} onChange={v => set('department', v)} options={opts.data.department} />
      <Select label="Categoría" value={filters.category} onChange={v => set('category', v)} options={opts.data.category} />
      <Select label="Métrica" value={filters.metric} onChange={v => set('metric', v)} options={Object.keys(labels)} all={false} format={v => labels[v]} />
      <Range label="Top" value={filters.limit} onChange={v => set('limit', v)} min={5} max={25} step={5} />
    </Filters>
    <div className="stat-grid four">
      <Stat label="Procesos" value={integer(d.totals.processes)} />
      <Stat label="Monto convocado" value={money(d.totals.tender_amount_pen)} tone="green" />
      <Stat label="Monto adjudicado" value={money(d.totals.award_amount_pen)} tone="orange" />
      <Stat label="Participación top 10" value={percent(d.top10_share)} note={`HHI ${integer(d.hhi)}`} tone="navy" />
    </div>
    <Card title="Evolución mensual" subtitle={`${labels[filters.metric]} para la selección actual`}><ExecutiveLine data={monthly} x="period" lines={[filters.metric]} formatter={formatter} /></Card>
    <div className="two-col">
      <Card title="Entidades principales" subtitle={labels[filters.metric]}><ExecutiveBar data={d.buyers} category="buyer_name" value={filters.metric} formatter={formatter} /></Card>
      <Card title="Distribución territorial" subtitle="Departamento de la entidad compradora"><ExecutiveBar data={d.departments} category="department" value={filters.metric} color="#de7c45" formatter={formatter} /></Card>
      <Card title="Categorías" subtitle={labels[filters.metric]}><ExecutiveBar data={d.categories} category="category" value={filters.metric} color="#384b72" formatter={formatter} /></Card>
      <Card title="Proveedores adjudicados" subtitle="Monto atribuido sin duplicar adjudicaciones"><ExecutiveBar data={d.suppliers} category="supplier_name" value="allocated_amount_pen" color="#93785e" formatter={money} /></Card>
    </div>
    <div className="insight"><span>Hallazgo para los filtros activos</span><p>Las diez entidades principales concentran <strong>{percent(d.top10_share)}</strong> de {labels[filters.metric].toLowerCase()}. El HHI descriptivo es <strong>{integer(d.hhi)}</strong>. Esta concentración describe la distribución y no demuestra irregularidad.</p></div>
    <Technical title="Detalle técnico: RDD, DataFrame y concentración">
      {technical.loading ? <Loading /> : technical.error ? <ErrorState error={technical.error} /> : <>
        <h4>Benchmark con resultados equivalentes</h4><DataTable rows={technical.data.benchmark} />
        <h4>Curva acumulada de entidades</h4><ExecutiveLine data={d.concentration} x="rank" lines={['share']} formatter={percent} />
        <h4>Distribución por rango de monto</h4><DataTable rows={technical.data.amount_distribution} />
        <h4>Sensibilidad a montos extremos</h4><DataTable rows={technical.data.outliers} />
        <details className="nested-detail"><summary>Planes de ejecución Spark</summary><pre>{JSON.stringify(technical.data.plans, null, 2)}</pre></details>
      </>}
    </Technical>
  </>
}
