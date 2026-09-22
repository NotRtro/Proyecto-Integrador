import { ArrowRight, CheckCircle2, Gauge, Layers3, ShieldCheck } from 'lucide-react'
import { api, integer, money, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveLine, Loading, SectionHeader, Stat, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Overview() {
  const { data, loading, error } = useAsync(() => api('/api/overview'), [])
  if (loading) return <Loading />
  if (error) return <ErrorState error={error} />
  const q = data.quality
  const yearly = data.yearly.map(row => ({ ...row, period: String(row.year) }))
  return <>
    <SectionHeader eyebrow="Panorama nacional" title="Contrataciones públicas, explicadas con evidencia"
      description="Una lectura ejecutiva de 198 mil procesos y cinco experimentos de minería de datos. Cada hallazgo puede abrirse para revisar cómo fue calculado."
      question="¿Qué aprendemos de las contrataciones publicadas entre 2023 y 2025?" />
    <div className="hero-panel">
      <div><span className="kicker">Cobertura analítica validada</span><h2>{integer(q.selected_processes)} procesos</h2><p>Convocatorias efectivas entre {q.min_date} y {q.max_date}. Los registros históricos fuera del periodo fueron separados.</p></div>
      <div className="hero-seal"><ShieldCheck /><strong>{Object.values(data.validation.checks).filter(Boolean).length}</strong><span>controles superados</span></div>
    </div>
    <div className="stat-grid four">
      <Stat label="Registros originales" value={integer(q.raw_records)} note="Tres archivos JSONL" />
      <Stat label="Procesos del periodo" value={integer(q.selected_processes)} note="2023–2025" tone="green" />
      <Stat label="Reglas interpretadas" value={data.selected_rules} note="Soporte y lift validados" tone="orange" />
      <Stat label="Estado del pipeline" value={data.run.status === 'complete' ? 'Completo' : data.run.status} note="Última ejecución" tone="navy" />
    </div>
    <div className="two-col executive-grid">
      <Card title="Evolución del volumen" subtitle="Procesos y montos convocados por año">
        <ExecutiveLine data={yearly} x="period" lines={['processes']} formatter={integer} />
      </Card>
      <Card title="Qué aportan las aproximaciones" subtitle="Resultados destacados de la evaluación">
        <div className="finding-list">
          <div><Gauge /><p><strong>LSH recupera candidatos similares</strong><span>Mejor F1 observado: {Number(data.best_lsh.f1).toFixed(3)}, con recall {percent(data.best_lsh.recall)}.</span></p></div>
          <div><Layers3 /><p><strong>HNSW acelera consultas vectoriales</strong><span>{Number(data.best_ann.speedup).toFixed(1)}× en su configuración destacada, con recall@10 de {percent(data.best_ann.recall_at_10)}.</span></p></div>
          <div><CheckCircle2 /><p><strong>Referencias exactas conservadas</strong><span>Las técnicas aproximadas se compararon con Jaccard, coseno y conteos exactos.</span></p></div>
        </div>
      </Card>
    </div>
    <Card title="La historia en cinco preguntas" subtitle="El recorrido ejecutivo del observatorio">
      <div className="question-grid">
        {[
          ['01', 'Concentración', '¿Dónde se concentra el monto y cómo cambia por año?'],
          ['02', 'Similitud', '¿Qué procesos comparten descripciones y qué errores introduce LSH?'],
          ['03', 'Vecinos', '¿Cuánto acelera HNSW la búsqueda y qué recall conserva?'],
          ['04', 'Flujos', '¿Cuánta memoria ahorran Bloom, CMS y DGIM?'],
          ['05', 'Asociaciones', '¿Qué características aparecen juntas con mayor frecuencia?'],
        ].map(([n, title, text]) => <div key={n}><span>{n}</span><strong>{title}</strong><p>{text}</p><ArrowRight size={18} /></div>)}
      </div>
    </Card>
    <Technical title="Detalle técnico de la ejecución">
      <div className="detail-grid"><div><b>Política temporal</b><p>{q.date_policy}</p></div><div><b>Conversión monetaria</b><p>{q.currency_policy}</p></div><div><b>Asignación a proveedores</b><p>{q.supplier_allocation}</p></div><div><b>Datos ausentes</b><p>{q.missing_amount_policy}</p></div><div><b>Valores extremos</b><p>{q.outlier_policy}</p></div><div><b>Categorías ausentes</b><p>{q.categorical_policy}</p></div></div>
      <h4>Totales anuales producidos con Spark SQL</h4><DataTable rows={data.yearly} />
      <h4>Etapas y tiempo de ejecución</h4><DataTable rows={data.run.stages} />
    </Technical>
  </>
}
