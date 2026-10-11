import { ArrowRight, ShieldCheck } from 'lucide-react'
import { api, integer, money, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveLine, Loading, SectionHeader, Stat, Technical } from '../components'
import { useAsync, useMeta } from '../hooks'

export default function Overview({ navigate }) {
  const { data, loading, error } = useAsync(() => api('/api/overview'), [])
  const meta = useMeta()
  if (error || meta.error) return <ErrorState error={error || meta.error} />
  if (loading || meta.loading) return <Loading />
  const q = data.quality
  const period = meta.data.period
  const yearly = data.yearly.map(row => ({ ...row, period: String(row.year) }))
  const local = data.buyer_hhi.filter(r => r.buyer_level === 'local').sort((a, b) => a.year - b.year)
  const consortia = q.suppliers?.types?.consorcio
  const singleRates = data.single_bidder.filter(r => r.year === 'Todos')
  const lsh = data.full_corpus_lsh || {}
  const questions = [
    ['q1', 'Q1 · Concentración', '¿Qué tan concentrada está la adjudicación en pocos proveedores dentro de cada entidad y departamento, y cambió entre años?',
      local.length > 1 ? `En municipalidades, las de concentración alta pasan de ${percent(local[0].high_share)} a ${percent(local.at(-1).high_share)}.` : ''],
    ['q2', 'Q2 · Competencia', '¿Qué características del proceso se asocian a tener un solo postor?',
      singleRates.length ? `Tasa de postor único por nivel de gobierno: de ${percent(Math.min(...singleRates.map(r => r.single_bidder_rate)))} a ${percent(Math.max(...singleRates.map(r => r.single_bidder_rate)))}.` : ''],
    ['q3', 'Q3 · Requerimientos similares', '¿Hay requerimientos casi idénticos convocados repetidamente por una entidad, o por muchas a la vez?',
      lsh.documents ? `LSH comparó ${integer(lsh.candidate_pairs)} candidatos en lugar de ${integer(lsh.possible_pairs)} pares y verificó ${integer(lsh.similar_pairs)} casi idénticos.` : ''],
  ]
  return <>
    <SectionHeader eyebrow="Contrataciones públicas del Perú" title="Concentración, competencia y requerimientos repetidos"
      description={`Minería de datos masivos sobre ${integer(q.selected_processes)} procesos de contratación publicados por el OECE en estándar OCDS (${period}). Tres preguntas de interés guían el uso de cada técnica del curso.`}
      question={`¿Qué revelan los datos abiertos de contratación ${period} sobre concentración, competencia y repetición de compras?`} />
    <div className="hero-panel">
      <div><span className="kicker">Cobertura analítica</span><h2>{integer(q.selected_processes)} procesos</h2><p>Convocatorias entre {q.min_date} y {q.max_date}, a partir de {integer(q.raw_records)} registros OCDS (versión más reciente de cada proceso).</p></div>
      <div className="hero-seal"><ShieldCheck /><strong>{Object.values(data.validation.checks).filter(Boolean).length}/{Object.keys(data.validation.checks).length}</strong><span>controles automáticos superados</span></div>
    </div>
    <div className="question-grid three">{questions.map(([id, title, text, evidence]) =>
      <button key={id} className="question-card" onClick={() => navigate(id)}>
        <strong>{title}</strong><p>{text}</p>{evidence && <small>{evidence}</small>}<ArrowRight size={18} />
      </button>)}
    </div>
    <div className="stat-grid four">
      <Stat label="Monto adjudicado" value={money(yearly.reduce((s, r) => s + (r.award_amount_pen || 0), 0))} note={period} />
      <Stat label="Consorcios" value={consortia ? percent(consortia.amount_share) : '—'} note="del monto atribuido a proveedores" tone="orange" />
      <Stat label="Mejor LSH (muestra)" value={`F1 ${Number(data.best_lsh.f1).toFixed(2)}`} note={`k=${data.best_lsh.hashes}, b=${data.best_lsh.bands}, r=${data.best_lsh.rows}`} tone="green" />
      <Stat label="HNSW frente a exacta" value={`${Number(data.best_ann.speedup).toFixed(0)}×`} note={`recall@10 ${percent(data.best_ann.recall_at_10)}`} tone="navy" />
    </div>
    <div className="two-col">
      <Card title="Procesos por año" subtitle="Convocatorias del periodo (Spark SQL)"><ExecutiveLine data={yearly} x="period" lines={['processes']} formatter={integer} /></Card>
      <Card title="Monto convocado y adjudicado por año" subtitle="Soles; montos extranjeros convertidos con tasas OECE"><ExecutiveLine data={yearly} x="period" lines={['tender_amount_pen', 'award_amount_pen']} formatter={money} /></Card>
    </div>
    <Technical title="Detalle técnico de la ejecución">
      <h4>Etapas del pipeline y duración</h4><DataTable rows={data.run.stages} />
      <h4>Controles de validación</h4><DataTable rows={Object.entries(data.validation.checks).map(([control, ok]) => ({ control, ok }))} />
    </Technical>
  </>
}
