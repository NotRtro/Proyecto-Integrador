import { decimal, integer, percent } from '../api'
import { Card, DataTable, ExecutiveBar, FindingModule, Stat } from '../components'

const minutes = seconds => `${decimal(seconds / 60, 1)} min`

// Decisiones de volumen: qué datos usa cada tarea, por qué, cuánto cuesta y qué se pierde al muestrear.
export default function Decisions({ d }) {
  if (!d) return null
  const q1 = d.evidence.q1_sampling || []
  const q3 = d.evidence.q3_sampling || []
  const ann = d.evidence.ann_scaling || {}
  const design = r => r.design || 'aleatorio'
  const q1Last = q1.filter(r => r.fraction === 0.1 && design(r) === 'aleatorio').at(-1)
  const q3Ten = q3.find(r => r.fraction === 0.1 && design(r) === 'aleatorio')
  const DESIGNS = { aleatorio: 'procesos al azar', conglomerados: 'entidades completas' }
  return <FindingModule id="datos-decisiones" part="Parte I · volumen y muestras" question="¿Qué tareas usan todos los datos, cuáles una muestra, y por qué?"
    pipeline={[['Tiempos de cada etapa', 'local', 'manifiesto de la corrida'], ['Efecto de muestrear Q1', 'Spark', 'HHI con 30 % y 10 %'], ['Efecto de muestrear Q3', 'local', 'pares que sobreviven'], ['Tabla de decisiones', 'local', 'metadata/data_decisions.json']]}>
    <div className="stat-grid four">
      <Stat label="Corrida" value={minutes(d.total_seconds)} note={`perfil ${d.profile || 'completo'} · experimentos: ${d.experiments_mode || 'si'}`} />
      {q1Last && <Stat label="Q1 con 10 % de muestra" value={percent(q1Last.buyers_kept_share)} note={`de las entidades evaluables en ${q1Last.year}`} tone="orange" />}
      {q3Ten && <Stat label="Q3 con 10 % de muestra" value={percent(q3Ten.pairs_kept_share)} note={`de los pares; ${integer(q3Ten.repeating_buyers_detected)} de ${integer(q3Ten.repeating_buyers_full)} entidades que repiten`} tone="navy" />}
      {ann.corpus_full && <Stat label="ANN con 50 mil" value={`recall ${percent(ann.hnsw_recall_reduced)}`} note={`total: ${percent(ann.hnsw_recall_full)}; aceleración ${decimal(ann.speedup_reduced, 0)}× frente a ${decimal(ann.speedup_full, 0)}×`} tone="green" />}
    </div>
    <Card title="Qué datos usa cada tarea" subtitle="Lo que responde Q1–Q3 usa todo; las referencias exactas y las comparaciones de algoritmos, una muestra justificada">
      <DataTable rows={d.tasks.map(t => ({ tarea: t.task, responde: t.question, datos: t.data, motor: t.engine, tiempo: `${decimal(t.seconds, 0)} s`, motivo: t.reason }))} />
    </Card>
    <div className="two-col">
      <Card title="Tiempo por etapa" subtitle="Esta corrida"><ExecutiveBar data={d.stages} category="name" value="seconds" formatter={v => `${decimal(v, 0)} s`} /></Card>
      <Card title="Mediciones de una vez" subtitle="Se repiten solo con --experiments si; si no, se reutilizan">
        <DataTable rows={d.experiments.map(e => ({ medición: e.name, estado: e.last_status, 'medida el': (e.measured_at || '').slice(0, 10), perfil: e.profile, segundos: e.seconds }))} />
      </Card>
    </div>
    {q1.length > 0 && <Card title="Q1: qué cambia si se muestrea" subtitle="HHI de proveedores por entidad (mínimo 5 adjudicaciones). Dos diseños: procesos al azar o entidades completas (conglomerados)">
      <DataTable rows={q1.map(r => ({ diseño: DESIGNS[design(r)], muestra: percent(r.fraction, 0), año: r.year, 'entidades evaluables': `${integer(r.buyers_sample)} de ${integer(r.buyers_full)}`, 'HHI mediano': `${integer(r.median_hhi_sample)} (total ${integer(r.median_hhi_full)})`, 'con HHI > 2 500': `${percent(r.high_share_sample)} (total ${percent(r.high_share_full)})`, 'mismo proveedor principal': percent(r.same_top_supplier), 'concentradas detectadas': r.high_recall == null ? '—' : percent(r.high_recall), 'alarmas correctas': r.high_precision == null ? '—' : percent(r.high_precision) }))} />
    </Card>}
    {q3.length > 0 && <Card title="Q3: qué cambia si se muestrea" subtitle="Con procesos al azar un par sobrevive si entran sus dos procesos (≈ p²); con entidades completas, si entran sus dos entidades">
      <DataTable rows={q3.map(r => ({ diseño: DESIGNS[design(r)], muestra: percent(r.fraction, 0), 'pares que sobreviven': `${integer(r.pairs_kept)} de ${integer(r.pairs_full)} (${percent(r.pairs_kept_share)})`, 'misma entidad': r.same_buyer_kept_share == null ? '—' : percent(r.same_buyer_kept_share), 'entre entidades': r.cross_buyer_kept_share == null ? '—' : percent(r.cross_buyer_kept_share), 'entidades que repiten detectadas': `${integer(r.repeating_buyers_detected)} de ${integer(r.repeating_buyers_full)}` }))} />
    </Card>}
  </FindingModule>
}
