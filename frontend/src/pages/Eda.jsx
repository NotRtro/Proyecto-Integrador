import { useState } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { STATIC, api, integer, money, percent } from '../api'
import {
  Card, CodeCompare, DataTable, ErrorState, ExecutiveBar, ExecutiveLine, Filters, FindingModule, Loading, Range,
  SectionHeader, Select, Stat, StaticNotice, Technical,
} from '../components'
import { useAsync, useMeta } from '../hooks'

const METRICS = { processes: 'Número de procesos', tender_amount_pen: 'Monto convocado', award_amount_pen: 'Monto adjudicado' }

function Histogram({ rows, year }) {
  const bins = Object.values(rows.filter(r => year === 'Todos' || String(r.year) === year).reduce((acc, r) => {
    const key = r.log10_bin.toFixed(2)
    acc[key] = acc[key] || { bin: r.log10_bin, label: money(r.bin_from_pen) }
    acc[key][r.category || 'sin dato'] = (acc[key][r.category || 'sin dato'] || 0) + r.processes
    return acc
  }, {})).sort((a, b) => a.bin - b.bin)
  const categories = [...new Set(rows.map(r => r.category || 'sin dato'))]
  const colors = ['#167c80', '#de7c45', '#384b72', '#93785e']
  return <div style={{ width: '100%', height: 320 }}><ResponsiveContainer>
    <BarChart data={bins} margin={{ top: 8, right: 16, bottom: 24, left: 8 }}>
      <CartesianGrid strokeDasharray="3 3" stroke="#e7edf2" vertical={false} />
      <XAxis dataKey="label" tick={{ fontSize: 10 }} interval={3} angle={-20} dy={10} />
      <YAxis tick={{ fontSize: 11 }} tickFormatter={integer} />
      <Tooltip formatter={integer} /><Legend />
      {categories.map((c, i) => <Bar key={c} dataKey={c} stackId="a" fill={colors[i % colors.length]} />)}
    </BarChart>
  </ResponsiveContainer></div>
}

function Partitioning({ experiment }) {
  if (!experiment) return null
  const { input, pruning, shuffle_partitions: shuffles, key_repartition: key } = experiment
  const fixed = shuffles.filter(r => !r.adaptive)
  const adaptive = shuffles.filter(r => r.adaptive)
  return <div className="prose">
    <p><strong>Lectura.</strong> Los {input.files} archivos .jsonl.gz producen {input.read_partitions} particiones: gzip no se puede dividir, así que cada archivo lo lee una sola tarea. Por eso el procesamiento reparticiona a {input.after_repartition} antes de parsear, para usar todos los núcleos del clúster.</p>
    <p><strong>Partition pruning.</strong> <code>processes</code> está escrito con <code>partitionBy(year, month)</code>. Filtrar por <code>year = {pruning.year}</code> lee {pruning.folders_read_with_pruning} de {pruning.folders_total} carpetas (el plan muestra <code>PartitionFilters: [{pruning.partition_column.partition_filters}]</code>) y tarda {pruning.partition_column.seconds.toFixed(2)} s. El mismo filtro expresado con la fecha selecciona las mismas {integer(pruning.rows)} filas, pero Spark debe abrir todos los archivos para revisar sus estadísticas (<code>PushedFilters</code>) y tarda {pruning.other_column.seconds.toFixed(2)} s.</p>
    <p><strong>Particiones del shuffle.</strong> Sin ejecución adaptativa (AQE), una agregación con {fixed[0]?.shuffle_partitions} particiones tarda {fixed[0]?.seconds.toFixed(2)} s y con {fixed.at(-1)?.shuffle_partitions} tarda {fixed.at(-1)?.seconds.toFixed(2)} s: demasiadas tareas pequeñas para este volumen. Con AQE, Spark fusiona el resultado en {adaptive[0]?.result_partitions} partición(es) y el valor configurado casi deja de importar ({adaptive.map(r => r.seconds.toFixed(2)).join(' / ')} s).</p>
    <p><strong>Repartition por clave.</strong> Para una sola agregación por entidad, reparticionar antes no ahorra shuffles ({key.single_aggregation.direct.exchanges} frente a {key.single_aggregation.repartition_first.exchanges}) y mueve filas completas en vez de agregados parciales ({key.single_aggregation.direct.seconds.toFixed(2)} s frente a {key.single_aggregation.repartition_first.seconds.toFixed(2)} s). Con dos agregaciones y un join sobre datos reparticionados y en caché, los shuffles bajan de {key.two_aggregations_and_join.direct.exchanges} a {key.two_aggregations_and_join.repartitioned_cached.exchanges}; a este volumen el ahorro no compensa leer la caché ({key.two_aggregations_and_join.repartitioned_cached.seconds.toFixed(2)} s), pero con datos mayores o más consultas sobre la misma clave sí.</p>
  </div>
}

export default function Eda() {
  const opts = useAsync(() => api('/api/eda/options'), [])
  const meta = useMeta()
  const [filters, setFilters] = useState({ year: 'Todos', month: 'Todos', department: 'Todos', category: 'Todos', modality: 'Todos', metric: 'processes' })
  const [top, setTop] = useState(10)
  const results = useAsync(() => api('/api/eda', { ...filters, limit: 25 }), Object.values(filters))
  const technical = useAsync(() => api('/api/eda/technical'), [])
  const set = (key, value) => setFilters(current => ({ ...current, [key]: value }))
  const error = opts.error || results.error || technical.error || meta.error
  if (error) return <ErrorState error={error} />
  if (opts.loading || results.loading || technical.loading || meta.loading) return <Loading />
  const d = results.data
  const t = technical.data
  const format = filters.metric === 'processes' ? integer : money
  const monthly = [...d.monthly].sort((a, b) => a.year - b.year || a.month - b.month).map(r => ({ ...r, period: `${r.year}-${String(r.month).padStart(2, '0')}` }))
  const slice = rows => rows.slice(0, top)
  const sparkRows = t.spark_vs_pandas.map(r => ({ ...r, fraction: `${r.fraction * 100}% (${integer(r.processes)})` }))
  return <>
    <SectionHeader eyebrow="Parte I · EDA en Spark" title="Cómo se distribuyen las compras públicas"
      description="Distribución temporal, por categoría, modalidad, territorio y monto, calculada con Spark DataFrames y Spark SQL y explorable con filtros."
      question="¿Cuándo, qué y cómo compra el Estado, y cuánto pesa cada segmento?" />
    <Filters>
      <Select label="Año" value={filters.year} onChange={v => set('year', v)} options={opts.data.year.map(String)} />
      {!STATIC && <Select label="Mes" value={filters.month} onChange={v => set('month', v)} options={opts.data.month.map(String)} />}
      <Select label="Departamento" value={filters.department} onChange={v => set('department', v)} options={opts.data.department} />
      {!STATIC && <Select label="Categoría" value={filters.category} onChange={v => set('category', v)} options={opts.data.category} />}
      {!STATIC && <Select label="Modalidad" value={filters.modality} onChange={v => set('modality', v)} options={opts.data.modality} />}
      <Select label="Métrica" value={filters.metric} onChange={v => set('metric', v)} options={Object.keys(METRICS)} all={false} format={v => METRICS[v]} />
      <Range label="Top" value={top} onChange={setTop} min={5} max={25} step={5} />
    </Filters>
    <StaticNotice data={d} />
    <div className="stat-grid three">
      <Stat label="Procesos" value={integer(d.totals.processes)} />
      <Stat label="Monto convocado" value={money(d.totals.tender_amount_pen)} tone="green" />
      <Stat label="Monto adjudicado" value={money(d.totals.award_amount_pen)} tone="orange" />
    </div>

    <FindingModule id="eda-temporal" part="Parte I · distribución temporal" question="¿Cómo se reparten las convocatorias a lo largo del año y entre años?"
      pipeline={[['processes (Parquet)', 'Spark', 'particionado por año/mes'], ['groupBy(año, mes)', 'Spark', 'monthly_evolution y cubo'], ['Filtro del cubo', 'local', 'API/navegador']]}>
      <Card title="Evolución mensual" subtitle={`${METRICS[filters.metric]} · ${meta.data.period}`}><ExecutiveLine data={monthly} x="period" lines={[filters.metric]} formatter={format} /></Card>
    </FindingModule>

    <FindingModule id="eda-categorias" part="Parte I · variables categóricas" question="¿Qué se compra, con qué modalidad y quién compra?"
      pipeline={[['processes', 'Spark'], ['groupBy por dimensión', 'Spark', 'categoría, modalidad, departamento, nivel'], ['Rankings', 'Spark', 'top entidades y proveedores']]}>
      <div className="two-col">
        <Card title="Modalidad de contratación" subtitle="Agrupada desde procurementMethodDetails"><ExecutiveBar data={slice(d.modalities)} category="modality" value={filters.metric} color="#384b72" formatter={format} /></Card>
        <Card title="Categoría" subtitle="Bienes, servicios y obras"><ExecutiveBar data={slice(d.categories)} category="category" value={filters.metric} formatter={format} /></Card>
        <Card title="Departamento del comprador" subtitle="Domicilio de la entidad, no lugar de ejecución"><ExecutiveBar data={slice(d.departments)} category="department" value={filters.metric} color="#de7c45" formatter={format} /></Card>
        <Card title="Nivel de gobierno" subtitle="Inferido del nombre de la entidad"><ExecutiveBar data={d.levels} category="buyer_level" value={filters.metric} color="#93785e" formatter={format} /></Card>
        <Card title="Entidades con más actividad" subtitle={METRICS[filters.metric]}><ExecutiveBar data={slice(d.buyers)} category="buyer_name" value={filters.metric} formatter={format} /></Card>
        <Card title="Proveedores" subtitle={d.supplier_metric === 'processes' ? 'Procesos adjudicados' : 'Monto atribuido (adjudicación repartida entre proveedores)'}><ExecutiveBar data={slice(d.suppliers)} category="supplier_name" value={d.supplier_metric} color="#93785e" formatter={d.supplier_metric === 'processes' ? integer : money} /></Card>
      </div>
    </FindingModule>

    <FindingModule id="eda-montos" part="Parte I · montos" question="¿Cómo se distribuyen los montos convocados?"
      pipeline={[['tender_amount_pen', 'Spark'], ['log10 y percentile_approx', 'Spark', 'histograma y percentiles por año y categoría']]}>
      <Card title="Histograma del monto convocado (escala logarítmica)" subtitle={`Procesos por intervalo de monto · ${filters.year === 'Todos' ? meta.data.period : filters.year}`}><Histogram rows={t.histogram} year={filters.year} /></Card>
      <Technical title="Percentiles y sensibilidad a montos extremos">
        <DataTable rows={t.percentiles.map(r => ({ año: r.year, categoría: r.category, procesos: r.processes, media: money(r.mean), p10: money(r.p10), p50: money(r.p50), p90: money(r.p90), p99: money(r.p99) }))} />
        <DataTable rows={t.outliers} />
      </Technical>
    </FindingModule>

    <FindingModule id="eda-spark" part="Parte I · RDD frente a DataFrame" question="¿Qué cambia al escribir la misma agregación como MapReduce (RDD) o como DataFrame?"
      pipeline={[['Misma entrada en caché', 'Spark'], ['RDD: map + reduceByKey', 'Spark', 'lambdas en Python'], ['DataFrame: groupBy + agg', 'Spark', 'optimizado por Catalyst'], ['Equivalencia + 3 corridas', 'Spark']]}>
      <Card title="Tiempo mediano por operación" subtitle="Mismo resultado verificado en ambos estilos">
        <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
          <BarChart data={Object.values(t.benchmark.reduce((acc, r) => { acc[r.operation] = { ...(acc[r.operation] || { operation: r.description }), [r.engine]: r.median_seconds }; return acc }, {}))} margin={{ left: 8, right: 16 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} /><XAxis dataKey="operation" tick={{ fontSize: 11 }} /><YAxis tickFormatter={v => `${v.toFixed(1)} s`} />
            <Tooltip formatter={v => `${Number(v).toFixed(3)} s`} /><Legend /><Bar dataKey="DataFrame" fill="#167c80" /><Bar dataKey="RDD" fill="#de7c45" />
          </BarChart></ResponsiveContainer></div>
      </Card>
      <DataTable rows={t.benchmark.map(r => ({ operación: r.description, estilo: r.engine, 'mediana (s)': r.median_seconds, shuffles: r.shuffles, líneas: r.code_lines, 'plan físico': r.operators }))} />
      <Card title="El código de la operación más compleja" subtitle={t.code.department_supplier_hhi?.description}><CodeCompare snippet={t.code.department_supplier_hhi} /></Card>
    </FindingModule>

    <FindingModule id="eda-particiones" part="Parte I · particionamiento" question="¿Cómo se particiona el dataset y cómo cambia eso el plan de ejecución?"
      pipeline={[['Lectura gzip', 'Spark'], ['Parquet por año/mes', 'Spark'], ['Consultas medidas', 'Spark', 'pruning, shuffle partitions, repartition']]}>
      <Partitioning experiment={t.partitioning} />
    </FindingModule>

    <FindingModule id="eda-spark-pandas" part="Parte I y VI · ¿hacía falta Spark?" question="¿Qué tan rápido es Spark frente a pandas en una agregación de este tamaño?"
      pipeline={[['Mismo Parquet', 'Spark'], ['Mismo Parquet', 'local', 'pandas + pyarrow'], ['groupBy año y categoría', 'Spark', 'y pandas']]}>
      <Card title="Tiempo de punta a punta según el tamaño" subtitle="Leer, filtrar la fracción, agrupar y traer el resultado">
        <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
          <LineChart data={sparkRows} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="fraction" tick={{ fontSize: 11 }} /><YAxis tickFormatter={v => `${v.toFixed(1)} s`} />
            <Tooltip formatter={v => `${Number(v).toFixed(2)} s`} /><Legend /><Line dataKey="spark_seconds" name="Spark" stroke="#de7c45" strokeWidth={2.5} /><Line dataKey="pandas_seconds" name="pandas" stroke="#167c80" strokeWidth={2.5} />
          </LineChart></ResponsiveContainer></div>
      </Card>
    </FindingModule>
  </>
}
