import { useState } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api, integer, money, percent } from '../api'
import { Card, DataTable, ErrorState, Filters, FindingModule, Loading, SectionHeader, Select, Stat } from '../components'
import { useAsync, useMeta } from '../hooks'

const LEVELS = ['local', 'regional', 'nacional', 'empresa_publica']
const COLORS = { local: '#de7c45', regional: '#167c80', nacional: '#384b72', empresa_publica: '#93785e' }

function pivot(rows, key, series, value) {
  return Object.values(rows.reduce((acc, r) => {
    acc[r[key]] = acc[r[key]] || { [key]: r[key] }
    acc[r[key]][r[series]] = r[value]
    return acc
  }, {})).sort((a, b) => String(a[key]).localeCompare(String(b[key])))
}

export default function Concentration() {
  const meta = useMeta()
  const [year, setYear] = useState('Todos')
  const [level, setLevel] = useState('Todos')
  const result = useAsync(() => api('/api/concentration', { year, buyer_level: level }), [year, level])
  if (result.error || meta.error) return <ErrorState error={result.error || meta.error} />
  if (result.loading || meta.loading) return <Loading />
  const d = result.data
  const market = d.market
  const scopes = [...new Set(market.map(r => r.scope))]
  const marketChart = pivot(market, 'year', 'scope', 'hhi')
  const summary = d.buyer_summary.filter(r => LEVELS.includes(r.buyer_level))
  const highShare = pivot(summary, 'year', 'buyer_level', 'high_share')
  const medianHhi = pivot(summary, 'year', 'buyer_level', 'median_hhi')
  const departments = [...d.departments].sort((a, b) => b.hhi - a.hhi).slice(0, 15)
  const consortia = d.supplier_types?.types?.consorcio
  const lastYear = String(meta.data.years.at(-1))
  const localLast = summary.find(r => r.buyer_level === 'local' && String(r.year) === lastYear)
  return <>
    <SectionHeader eyebrow="Q1 · Concentración" title="¿Dependen las entidades de pocos proveedores?"
      description="El mercado nacional está atomizado, pero la pregunta relevante es dentro de cada entidad compradora: cuánto del monto adjudicado se lleva su principal proveedor y cómo cambia entre años y niveles de gobierno."
      question="¿Qué tan concentrada está la adjudicación en pocos proveedores dentro de cada entidad y departamento, y cambió entre años?" />
    <Filters>
      <Select label="Año" value={year} onChange={setYear} options={meta.data.years.map(String)} />
      <Select label="Nivel de gobierno" value={level} onChange={setLevel} options={LEVELS} />
    </Filters>
    <div className="stat-grid four">
      <Stat label="Consorcios" value={consortia ? percent(consortia.amount_share) : '—'} note="del monto atribuido a proveedores" tone="orange" />
      <Stat label="Municipalidades con HHI > 2 500" value={localLast ? percent(localLast.high_share) : '—'} note={`${lastYear}, con 5+ adjudicaciones`} />
      <Stat label="HHI mediano municipal" value={localLast ? integer(localLast.median_hhi) : '—'} note={lastYear} tone="green" />
      <Stat label="Proveedor principal (municipal)" value={localLast ? percent(localLast.mean_top1_share) : '—'} note="participación media" tone="navy" />
    </div>

    <FindingModule id="q1-mercado" part="Parte I · HHI nacional" question="¿El mercado nacional de contratación está concentrado?"
      pipeline={[['suppliers + processes', 'Spark', 'monto atribuido por proveedor'], ['Ventana por año', 'Spark', 'participación, HHI, top-10'], ['Con y sin consorcios', 'Spark']]}>
      <Card title="HHI por año" subtitle="0 = atomizado · 1 500 moderado · 2 500 alto · 10 000 = un solo actor">
        <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
          <BarChart data={marketChart} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" vertical={false} /><XAxis dataKey="year" /><YAxis tickFormatter={integer} />
            <Tooltip formatter={v => integer(v)} /><Legend />{scopes.map((s, i) => <Bar key={s} dataKey={s} fill={['#167c80', '#de7c45', '#384b72'][i % 3]} />)}
          </BarChart></ResponsiveContainer></div>
      </Card>
      <DataTable rows={market.map(r => ({ alcance: r.scope, año: r.year, actores: r.entities, HHI: r.hhi, 'top 10': percent(r.top10_share), total: money(r.total) }))} />
    </FindingModule>

    <FindingModule id="q1-entidades" part="Parte I · HHI por entidad" question="¿Cuántas entidades concentran su gasto en pocos proveedores, y va en aumento?"
      pipeline={[['suppliers por entidad y año', 'Spark'], ['HHI por entidad', 'Spark', 'mínimo 5 adjudicaciones'], ['Resumen por nivel', 'Spark', 'mediana y % con HHI > 2 500']]}>
      <div className="two-col">
        <Card title="Entidades con concentración alta" subtitle="Proporción con HHI > 2 500, por nivel de gobierno">
          <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
            <LineChart data={highShare} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="year" /><YAxis tickFormatter={v => percent(v, 0)} />
              <Tooltip formatter={v => percent(v)} /><Legend />{LEVELS.map(l => <Line key={l} dataKey={l} stroke={COLORS[l]} strokeWidth={2.5} />)}
            </LineChart></ResponsiveContainer></div>
        </Card>
        <Card title="HHI mediano por nivel" subtitle="Entidades con al menos 5 adjudicaciones">
          <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
            <LineChart data={medianHhi} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="year" /><YAxis tickFormatter={integer} />
              <Tooltip formatter={v => integer(v)} /><Legend />{LEVELS.map(l => <Line key={l} dataKey={l} stroke={COLORS[l]} strokeWidth={2.5} />)}
            </LineChart></ResponsiveContainer></div>
        </Card>
      </div>
      <Card title="Entidades más dependientes de un proveedor" subtitle={`Ordenadas por HHI · año ${d.year_shown}`}>
        <DataTable rows={d.most_dependent.map(r => ({ entidad: r.buyer_name, nivel: r.buyer_level, departamento: r.department, año: r.year, adjudicaciones: r.awards, proveedores: r.entities, HHI: r.hhi, 'proveedor principal': r.top_supplier, tipo: r.top_supplier_type, participación: percent(r.top1_share), monto: money(r.total) }))} />
      </Card>
    </FindingModule>

    <FindingModule id="q1-departamentos" part="Parte I · territorio" question="¿En qué departamentos se concentra más la adjudicación, y cuánto cambia al excluir los consorcios?"
      pipeline={[['suppliers + departamento del comprador', 'Spark', 'join por ocid'], ['HHI por departamento', 'Spark', 'con y sin consorcios']]}>
      <Card title="Departamentos con mayor HHI de proveedores" subtitle={`Año ${d.year_shown}`}>
        <div style={{ width: '100%', height: 360 }}><ResponsiveContainer>
          <BarChart data={departments} layout="vertical" margin={{ left: 20, right: 20 }}><CartesianGrid strokeDasharray="3 3" horizontal={false} />
            <XAxis type="number" tickFormatter={integer} /><YAxis dataKey="department" type="category" width={110} tick={{ fontSize: 11 }} />
            <Tooltip formatter={v => integer(v)} /><Legend /><Bar dataKey="hhi" name="Todos los proveedores" fill="#167c80" /><Bar dataKey="hhi_sin_consorcios" name="Sin consorcios" fill="#de7c45" />
          </BarChart></ResponsiveContainer></div>
      </Card>
    </FindingModule>

    <FindingModule id="q1-repetidos" part="Parte IV · Bloom (referencia exacta)" question="¿Con qué frecuencia una adjudicación va a un proveedor que ya había ganado con esa misma entidad?"
      pipeline={[['Flujo de adjudicaciones', 'Spark', 'ordenado por fecha'], ['Par (entidad, proveedor)', 'local', '¿ya visto? (exacto y Bloom)'], ['Tasa por año', 'local']]}>
      <div className="two-col">
        <Card title="Adjudicaciones a proveedores que repiten con la entidad" subtitle="Conteo exacto sobre el flujo ordenado; el Laboratorio compara con el Bloom Filter">
          <DataTable rows={d.repeat_winners.map(r => ({ año: r.year, adjudicaciones: r.events, repiten: r.repeats, tasa: percent(r.repeat_rate) }))} />
        </Card>
        <Card title="Principales proveedores" subtitle={`Monto atribuido y tipo de proveedor · año ${d.year_shown}`}>
          <DataTable rows={d.top_suppliers.map(r => ({ año: r.year, proveedor: r.supplier_name, tipo: r.supplier_type, participación: percent(r.share), monto: money(r.value) }))} limit={15} />
        </Card>
      </div>
      <div className="callout"><strong>Lectura del efecto acumulado</strong><p>La tasa crece con el tiempo en parte por construcción: al inicio del flujo ningún par entidad–proveedor se ha visto. Comparar años completos y no meses iniciales.</p></div>
    </FindingModule>
  </>
}
