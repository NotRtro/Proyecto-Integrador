import { api, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, FindingModule, Loading, SectionHeader, Stat, Technical } from '../components'
import { useAsync, useMeta } from '../hooks'
import Decisions from './Decisions'

const LEVELS = { local: 'Local', regional: 'Regional', nacional: 'Nacional', empresa_publica: 'Empresa pública', otro: 'Otro' }
const SUPPLIERS = { persona_juridica: 'Persona jurídica', persona_natural: 'Persona natural', consorcio: 'Consorcio', extranjero: 'Extranjero', otro: 'Otro' }

export default function Data() {
  const { data, loading, error } = useAsync(() => api('/api/quality'), [])
  const meta = useMeta()
  if (error || meta.error) return <ErrorState error={error || meta.error} />
  if (loading || meta.loading) return <Loading />
  const q = data.quality
  const years = meta.data.years.map(String)
  const coverage = Object.values(data.coverage.filter(r => years.includes(String(r.effective_year))).reduce((acc, r) => {
    const year = String(r.effective_year); acc[year] = acc[year] || { year, count: 0 }; acc[year].count += r.count; return acc
  }, {}))
  const nulls = Object.values(data.nulls.reduce((acc, r) => {
    acc[r.field] = acc[r.field] || { field: r.field, percentage: 0 }; acc[r.field].percentage = Math.max(acc[r.field].percentage, r.percentage); return acc
  }, {})).sort((a, b) => b.percentage - a.percentage)
  const levels = Object.entries(q.buyer_levels?.levels || {}).map(([level, v]) => ({ level: LEVELS[level] || level, ...v }))
    .sort((a, b) => b.processes - a.processes)
  const suppliers = Object.entries(q.suppliers?.types || {}).map(([type, v]) => ({ type: SUPPLIERS[type] || type, ...v }))
    .sort((a, b) => b.amount_share - a.amount_share)
  const schema = Object.entries(data.schema).map(([table, s]) => ({ tabla: table, columnas: s.fields.length, campos: s.fields.map(f => f.name).join(', ') }))
  const stages = [
    ['Selección', `${meta.data.source.files} archivos OCDS anuales (${integer(q.raw_records)} registros). Año por fecha de convocatoria, no por archivo.`],
    ['Preprocesamiento', `${integer(q.duplicate_ocids)} versiones duplicadas de procesos eliminadas (última por ocid); ${integer(q.outside_period_or_missing_date)} fuera del periodo; montos extranjeros convertidos a soles con tasas OECE.`],
    ['Transformación', 'Tablas Parquet particionadas por año y mes; nivel de gobierno, modalidad peruana, tipo de proveedor, bandas de monto, competencia y shingles de texto.'],
    ['Minería', 'EDA y concentración en Spark; MinHash/LSH; HNSW e IVF; Bloom, Count-Min y DGIM; Apriori y FP-Growth.'],
    ['Interpretación', 'Cada hallazgo responde a Q1, Q2 o Q3 con sus límites; la discusión crítica pondera precisión, memoria, tiempo y uso responsable.'],
  ]
  return <>
    <SectionHeader eyebrow="Parte I · KDD" title={`De ${integer(q.raw_records)} documentos OCDS a tablas analíticas`}
      description="Qué entró al análisis, qué se descartó y por qué, y cómo quedó el esquema final que usan todas las partes."
      question="¿Qué datos sostienen las conclusiones y cuáles son sus límites?" />
    <div className="kdd">{stages.map(([title, text], i) => <div key={title}><b>{i + 1}. {title}</b><p>{text}</p></div>)}</div>
    <div className="stat-grid four">
      <Stat label="Registros OCDS" value={integer(q.raw_records)} note={`Archivos ${meta.data.period}`} />
      <Stat label="Procesos analizados" value={integer(q.selected_processes)} note={`${q.min_date} — ${q.max_date}`} tone="green" />
      <Stat label="Entidades clasificadas" value={percent(q.buyer_levels?.classified_share)} note="por nivel de gobierno" tone="orange" />
      <Stat label="RUC con varios nombres" value={integer(q.suppliers?.ids_with_name_variants)} note="unificados al más reciente" tone="navy" />
    </div>

    <FindingModule id="datos-calidad" part="Parte I · limpieza" question="¿Qué tan completo es el registro y qué decisiones de limpieza cambian los resultados?"
      pipeline={[['Validación de archivos', 'local', 'SHA-256, JSON válido, cobertura por año'], ['Parseo y staging', 'Spark', 'una pasada, Parquet'],
        ['Deduplicación y periodo', 'Spark', 'última versión por ocid'], ['Conversión de moneda', 'Spark', 'tasas OECE diaria/anual/periodo'], ['Reporte de calidad', 'Spark', 'nulos por año, granos, coberturas']]}>
      <div className="two-col">
        <Card title="Procesos por año de convocatoria" subtitle="Los archivos anuales también traen procesos antiguos recompilados"><ExecutiveBar data={coverage} category="year" value="count" formatter={integer} /></Card>
        <Card title="Campos con más vacíos" subtitle="Mayor porcentaje observado entre años"><ExecutiveBar data={nulls} category="field" value="percentage" color="#de7c45" formatter={v => `${Number(v).toFixed(1)}%`} /></Card>
      </div>
      <div className="two-col">
        <Card title="Nivel de gobierno del comprador" subtitle="Inferido del nombre oficial con reglas (el OCDS no lo publica)"><DataTable rows={levels.map(r => ({ nivel: r.level, entidades: r.entities, procesos: r.processes, participación: percent(r.share) }))} /></Card>
        <Card title="Tipo de proveedor" subtitle="Por prefijo del RUC; los consorcios ocultan a sus integrantes"><DataTable rows={suppliers.map(r => ({ tipo: r.type, identificadores: r.ids, adjudicaciones: r.rows, 'monto atribuido': percent(r.amount_share) }))} /></Card>
      </div>
    </FindingModule>

    <Decisions d={data.decisions} />

    <Technical title="Decisiones de limpieza, esquema y tiempos">
      <div className="detail-grid">
        <div><b>Fechas</b><p>{q.date_policy}</p></div>
        <div><b>Duplicados</b><p>{q.duplicate_policy}</p></div>
        <div><b>Monedas</b><p>{q.currency_policy}</p></div>
        <div><b>Montos faltantes</b><p>{q.missing_amount_policy}</p></div>
        <div><b>Valores extremos</b><p>{q.outlier_policy}</p></div>
        <div><b>Proveedores</b><p>{q.suppliers?.policy}</p></div>
      </div>
      <h4>Stopwords de dominio (frecuencia documental &gt; {percent(data.stopwords?.threshold, 0)})</h4>
      <p>{data.stopwords?.terms?.map(t => `${t.term} (${percent(t.document_frequency)})`).join(', ')}. El título OCDS es la nomenclatura del proceso y se excluye del texto.</p>
      <h4>Esquema final (Parquet particionado por año y mes)</h4><DataTable rows={schema} />
      <h4>Conversión de montos de adjudicación</h4><DataTable rows={Object.entries(q.monetary_coverage.awards_by_conversion_method).map(([method, v]) => ({ método: method, adjudicaciones: v.awards, monto: v.amount_pen }))} />
      <h4>Archivos fuente</h4><DataTable rows={data.sources.map(s => ({ archivo: s.file, registros: s.records, bytes: s.bytes, sha256: s.sha256?.slice(0, 16), publicado: s.published_at }))} />
      <h4>Tiempo de cada subetapa del procesamiento (Spark)</h4><DataTable rows={data.processing_timings} />
    </Technical>
  </>
}
