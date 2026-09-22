import { api, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Loading, SectionHeader, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Quality() {
  const { data, loading, error } = useAsync(() => api('/api/quality'), [])
  if (loading) return <Loading />
  if (error) return <ErrorState error={error} />
  const q = data.quality
  const periodCoverage = Object.values(data.coverage.filter(row => ['2023', '2024', '2025'].includes(String(row.effective_year))).reduce((acc, row) => {
    const year = String(row.effective_year)
    acc[year] = acc[year] || { effective_year: year, count: 0 }
    acc[year].count += row.count
    return acc
  }, {}))
  const nullSummary = Object.values(data.nulls.reduce((acc, row) => {
    const key = row.field; acc[key] = acc[key] || { field: key, percentage: 0 }
    acc[key].percentage = Math.max(acc[key].percentage, row.percentage); return acc
  }, {})).sort((a, b) => b.percentage - a.percentage)
  return <>
    <SectionHeader eyebrow="Gobierno del dato" title="Cobertura y calidad antes de interpretar"
      description="Los archivos se validan, los procesos se ubican por su fecha efectiva y cada monto conserva trazabilidad al OCID original."
      question="¿Qué entró al análisis, qué quedó fuera y cuáles son sus limitaciones?" />
    <div className="stat-grid four">
      <Stat label="Objetos válidos" value={integer(q.raw_records)} note="JSON recuperados" />
      <Stat label="Periodo analizado" value={integer(q.selected_processes)} note={`${q.min_date} — ${q.max_date}`} tone="green" />
      <Stat label="Fuera del periodo" value={integer(q.outside_period_or_missing_date)} note="Registros históricos" tone="orange" />
      <Stat label="Moneda no PEN" value={integer(q.non_pen)} note="Conversión trazable" tone="navy" />
    </div>
    <div className="two-col">
      <Card title="Cobertura temporal real" subtitle="Objetos por año efectivo y archivo de procedencia">
        <ExecutiveBar data={periodCoverage} category="effective_year" value="count" formatter={integer} />
      </Card>
      <Card title="Campos que limitan el análisis" subtitle="Mayor porcentaje de ausencia observado entre años">
        <ExecutiveBar data={nullSummary} category="field" value="percentage" color="#de7c45" formatter={v => `${Number(v).toFixed(1)}%`} />
      </Card>
    </div>
    <div className="callout"><strong>Lectura responsable</strong><p>Un monto ausente no representa cero gasto. El departamento identifica al comprador y no necesariamente el lugar de ejecución. Los procesos recompilados describen su estado más reciente.</p></div>
    <Technical title="Inventario, hashes y controles de calidad">
      <h4>Cobertura y decisiones monetarias</h4>
      <div className="detail-grid">
        <div><b>Conversión</b><p>{q.currency_policy}</p></div>
        <div><b>Ausencias</b><p>{q.missing_amount_policy}</p></div>
        <div><b>Valores extremos</b><p>{q.outlier_policy}</p></div>
      </div>
      <DataTable rows={Object.entries(q.monetary_coverage.awards_by_conversion_method).map(([method, values]) => ({ method, ...values }))} />
      <h4>Archivos fuente</h4><DataTable rows={data.sources} />
      <h4>Granos de las tablas procesadas</h4><DataTable rows={Object.entries(q.table_counts).map(([table, count]) => ({ table, count }))} />
      <h4>Nulos por campo y año</h4><DataTable rows={data.nulls} />
      <details className="nested-detail"><summary>Esquema analítico JSON</summary><pre>{JSON.stringify(data.schema, null, 2)}</pre></details>
    </Technical>
  </>
}
