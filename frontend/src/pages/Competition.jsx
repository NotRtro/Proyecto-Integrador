import { useState } from 'react'
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { api, decimal, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, FindingModule, Loading, SectionHeader, Select, Stat } from '../components'
import { useAsync, useMeta } from '../hooks'
import Participation from './Participation'

const DIMENSIONS = { modality: 'Modalidad', category: 'Categoría', department: 'Departamento', amount_band: 'Banda de monto', buyer_level: 'Nivel de gobierno' }

export default function Competition() {
  const meta = useMeta()
  const [dimension, setDimension] = useState('modality')
  const [year, setYear] = useState('Todos')
  const [windowKey, setWindowKey] = useState('eventos|1000')
  const result = useAsync(() => api('/api/competition', { dimension, year }), [dimension, year])
  if (result.error || meta.error) return <ErrorState error={result.error || meta.error} />
  if (result.loading || meta.loading) return <Loading />
  const d = result.data
  const rates = d.rates.filter(r => r.informed >= 50)
  const overall = rates.reduce((acc, r) => ({ single: acc.single + r.single_bidder, informed: acc.informed + r.informed, total: acc.total + r.processes }), { single: 0, informed: 0, total: 0 })
  const leaders = rates.slice(0, 5).map(r => r.value)
  const trend = Object.values(d.by_year.filter(r => leaders.includes(r.value)).reduce((acc, r) => {
    acc[r.year] = acc[r.year] || { year: r.year }; acc[r.year][r.value] = r.single_bidder_rate; return acc
  }, {})).sort((a, b) => a.year.localeCompare(b.year))
  const single = d.rules.filter(r => r.consequent === 'POSTORES=uno').slice(0, 15)
  const contrast = d.rules.filter(r => r.consequent === 'POSTORES=cuatro o mas').slice(0, 8)
  const [unit, size] = windowKey.split('|')
  const dgimRow = d.dgim.find(r => r.unit === unit && String(r.window) === size)
  const series = d.dgim_series.filter(r => r.unit === unit && String(r.window) === size)
  return <>
    <SectionHeader eyebrow="Q2 · Competencia" title="¿Dónde se presenta un solo postor?"
      description="Un proceso con un solo postor no compite: el precio no se contrasta. Se mide la tasa por modalidad, territorio, monto y nivel de gobierno; las reglas de asociación buscan combinaciones que la elevan y DGIM vigila la proporción reciente."
      question="¿Qué características del proceso se asocian a tener un solo postor?" />
    <Filters>
      <Select label="Dimensión" value={dimension} onChange={setDimension} options={Object.keys(DIMENSIONS)} all={false} format={v => DIMENSIONS[v]} />
      <Select label="Año" value={year} onChange={setYear} options={meta.data.years.map(String)} />
    </Filters>
    <div className="stat-grid three">
      <Stat label="Tasa de postor único" value={percent(overall.single / Math.max(1, overall.informed))} note="sobre procesos con postores informados" />
      <Stat label="Sin dato de postores" value={percent(1 - overall.informed / Math.max(1, overall.total))} note="excluidos de la tasa" tone="orange" />
      <Stat label="Procesos con un postor" value={integer(overall.single)} note={year === 'Todos' ? meta.data.period : year} tone="green" />
    </div>

    <FindingModule id="q2-tasas" part="Parte I · tasas por dimensión" question={`¿Cómo varía la tasa de postor único según ${DIMENSIONS[dimension].toLowerCase()}?`}
      pipeline={[['processes', 'Spark'], ['groupBy dimensión (y año)', 'Spark', 'postor único / informados'], ['Sin dato aparte', 'Spark']]}>
      <div className="two-col">
        <Card title="Tasa de postor único" subtitle="Grupos con al menos 50 procesos informados"><ExecutiveBar data={rates.slice(0, 15)} category="value" value="single_bidder_rate" formatter={v => percent(v)} height={360} /></Card>
        <Card title="Evolución en los cinco grupos con mayor tasa" subtitle="Por año de convocatoria">
          <div style={{ width: '100%', height: 360 }}><ResponsiveContainer>
            <LineChart data={trend} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="year" /><YAxis tickFormatter={v => percent(v, 0)} />
              <Tooltip formatter={v => percent(v)} /><Legend />{leaders.map((v, i) => <Line key={v} dataKey={v} stroke={['#167c80', '#de7c45', '#384b72', '#93785e', '#4c91b6'][i]} strokeWidth={2.5} />)}
            </LineChart></ResponsiveContainer></div>
        </Card>
      </div>
      <DataTable rows={rates.map(r => ({ grupo: r.value, procesos: r.processes, informados: r.informed, 'un postor': r.single_bidder, tasa: percent(r.single_bidder_rate), 'sin dato': percent(r.missing_share) }))} />
      {d.bidders?.length > 0 && <Card title="¿Qué cuenta el número de postores?" subtitle="tender.numberOfTenderers por modalidad: valores tan altos corresponden a participantes registrados, no a ofertas">
        <DataTable rows={d.bidders.map(r => ({ modalidad: r.modality, procesos: r.processes, mediana: r.median, p90: r.p90, máximo: r.max, 'un participante': percent(r.single_share), '10 o más': percent(r.ten_or_more_share) }))} limit={12} />
      </Card>}
    </FindingModule>

    <Participation d={d} />

    <FindingModule id="q2-reglas" part="Parte V · reglas dirigidas" question="¿Qué combinaciones de características elevan la probabilidad de tener un solo postor?"
      pipeline={[['Canastas por proceso', 'Spark', 'categoría, modalidad, departamento, monto, postores, nivel'], ['Sin modalidades no competitivas', 'Spark', 'más de 50 % de postor único: directa, internacional, convenios…'], ['FP-Growth (soporte 0,2 %)', 'Spark'], ['Reglas A → POSTORES=uno', 'local', 'lift frente a la tasa base']]}>
      <Card title="Combinaciones que elevan el postor único" subtitle="Lift = cuántas veces más probable que la tasa base, solo en modalidades competitivas">
        <DataTable rows={single.map(r => ({ combinación: r.antecedent, procesos: r.antecedent_count, 'con un postor': r.count, confianza: percent(r.confidence), 'tasa base': percent(r.base_rate), lift: decimal(r.lift), improvement: r.improvement == null ? '—' : percent(r.improvement, 2), tipo: r.kind }))} />
      </Card>
      <Card title="Contraste: combinaciones con cuatro o más postores" subtitle="Dónde la competencia es más intensa">
        <DataTable rows={contrast.map(r => ({ combinación: r.antecedent, procesos: r.antecedent_count, confianza: percent(r.confidence), lift: decimal(r.lift) }))} />
      </Card>
    </FindingModule>

    <FindingModule id="q2-ventanas" part="Parte IV · DGIM" question="¿Cuántos procesos con un solo postor hubo en la ventana reciente, sin guardar todo el historial?"
      pipeline={[['Flujo de adjudicaciones', 'Spark', 'ordenado por fecha'], ['Bit = postor único', 'local'], ['DGIM por ventana', 'local', 'eventos o días'], ['Conteo exacto paralelo', 'local', 'para medir el error']]}>
      <Filters><Select label="Ventana" value={windowKey} onChange={setWindowKey} all={false}
        options={d.dgim.map(r => `${r.unit}|${r.window}`)} format={v => { const [u, s] = v.split('|'); return `Últimos ${integer(s)} ${u}` }} /></Filters>
      {dgimRow && <div className="stat-grid four">
        <Stat label="Conteo exacto" value={integer(dgimRow.exact)} note="al final del flujo" />
        <Stat label="Estimación DGIM" value={decimal(dgimRow.estimate, 1)} tone="green" />
        <Stat label="Error relativo" value={dgimRow.relative_error == null ? '—' : percent(dgimRow.relative_error)} note="cota teórica 50 %" tone="orange" />
        <Stat label="Memoria" value={`${dgimRow.buckets} buckets`} note={`${dgimRow.bytes} B frente a ${integer(dgimRow.exact_bytes)} B exactos`} tone="navy" />
      </div>}
      <Card title="Procesos con un solo postor en la ventana" subtitle="Exacto frente a DGIM a lo largo del flujo">
        <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
          <LineChart data={series} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="date" tick={{ fontSize: 10 }} minTickGap={40} /><YAxis tickFormatter={integer} />
            <Tooltip /><Legend /><Line dataKey="exact" name="Exacto" stroke="#384b72" dot={false} strokeWidth={2} /><Line dataKey="estimate" name="DGIM" stroke="#de7c45" dot={false} strokeWidth={2} />
          </LineChart></ResponsiveContainer></div>
      </Card>
    </FindingModule>
  </>
}
