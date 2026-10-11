import { useMemo, useState } from 'react'
import { api, decimal, integer, percent } from '../../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, FindingModule, Loading, Range, Select, Stat } from '../../components'
import { useAsync, useMeta } from '../../hooks'
import interpretations from '../../content/rules.json'

const KINDS = ['informativa', 'normativa', 'datos faltantes']

export default function RulesLab() {
  const meta = useMeta()
  const [year, setYear] = useState('Todos')
  const [support, setSupport] = useState(0.02)
  const [confidence, setConfidence] = useState(0.5)
  const [lift, setLift] = useState(1.2)
  const [kind, setKind] = useState('informativa')
  const result = useAsync(() => api('/api/association', { year }), [year])
  // Filtrado en vivo de reglas ya calculadas (lo que sugiere el enunciado): no se vuelve a minar.
  const rules = useMemo(() => (result.data?.rules || []).filter(r => r.support >= support && r.confidence >= confidence
    && r.lift >= lift && (kind === 'Todos' || r.kind === kind)), [result.data, support, confidence, lift, kind])
  if (result.error || meta.error) return <ErrorState error={result.error || meta.error} />
  if (result.loading || meta.loading) return <Loading />
  const d = result.data
  const kinds = KINDS.map(k => ({ tipo: k, reglas: d.rules.filter(r => r.kind === k).length }))
  const sample = d.benchmark.filter(r => r.scope === 'same_sample')
  return <FindingModule id="lab-reglas" part="Parte V · Apriori y FP-Growth" question="¿Qué características de los procesos aparecen juntas más de lo esperable?"
    pipeline={[['Canasta por proceso', 'Spark', 'CAT, MODALIDAD, DEP, MONTO, POSTORES, NIVEL'], ['Apriori propio (muestra)', 'local', 'referencia'], ['FP-Growth (todas las canastas)', 'Spark', 'MLlib'], ['Reglas por año y tipo', 'local']]}>
    <div className="lab">
      <Filters>
        <Select label="Año" value={year} onChange={setYear} options={meta.data.years.map(String)} />
        <Select label="Tipo de regla" value={kind} onChange={setKind} options={KINDS} />
        <Range label="Soporte mínimo" value={support} onChange={setSupport} min={0.02} max={0.2} step={0.01} />
        <Range label="Confianza mínima" value={confidence} onChange={setConfidence} min={0.5} max={1} step={0.05} />
        <Range label="Lift mínimo" value={lift} onChange={setLift} min={1} max={10} step={0.1} />
        <p className="hint">Normativa: relaciona solo modalidad, monto y categoría, o predice la modalidad desde el objeto o el monto (lo fija la ley), o contratación directa con un postor. Datos faltantes: incluye "sin dato". Improvement: cuánto supera la confianza de la regla a la de su mejor versión más simple.</p>
      </Filters>
      <div>
        <div className="stat-grid three">
          <Stat label="Reglas visibles" value={integer(rules.length)} note={`de ${integer(d.total_rules)} del año`} />
          <Stat label="Canastas" value={integer(d.selected.transactions)} tone="green" />
          <Stat label="Apriori = FP-Growth" value={sample.every(r => r.equivalent) ? 'Sí' : 'No'} note="mismos conjuntos en la muestra" tone="orange" />
        </div>
        <Card title="Reglas con mayor lift" subtitle="Bajo los filtros actuales"><ExecutiveBar data={rules.slice(0, 12).map((r, i) => ({ ...r, label: `R${i + 1}` }))} category="label" value="lift" formatter={v => decimal(v)} height={300} /></Card>
      </div>
    </div>
    <DataTable rows={rules.map((r, i) => ({ id: `R${i + 1}`, si: r.antecedent, entonces: r.consequent, soporte: percent(r.support), confianza: percent(r.confidence), lift: decimal(r.lift), improvement: r.improvement == null ? '—' : decimal(r.improvement, 3), procesos: r.count, tipo: r.kind }))} />
    <Card title="Reglas seleccionadas para interpretar" subtitle={`${d.selected.found} reglas: generales informativas (improvement ≥ 5 puntos) y de postor único (Q2), sin redundancias`}>
      {d.selected.rules.map(rule => <div className="selected-rule" key={rule.rule_id}>
        <span>{rule.source === 'general' ? 'G' : 'Q2'}</span>
        <div><p><strong>{rule.rule_id}</strong></p><p className="hint">{rule.automatic_reading}</p>
          <p>{interpretations.reglas[rule.rule_id] || <em>Interpretación pendiente en frontend/src/content/rules.json.</em>}</p></div>
      </div>)}
    </Card>
    <div className="two-col">
      <Card title="Apriori frente a FP-Growth" subtitle="Misma muestra; FP-Growth además sobre todas las canastas">
        <DataTable rows={d.benchmark.map(r => ({ algoritmo: r.algorithm, alcance: r.scope === 'full' ? 'todas' : 'muestra', soporte: percent(r.support, 0), canastas: r.transactions, conjuntos: r.itemsets, 'segundos': decimal(r.seconds, 2) }))} />
      </Card>
      <Card title="Reglas por tipo y calidad de las canastas" subtitle="Proporción de 'sin dato' por atributo">
        <DataTable rows={kinds} />
        <DataTable rows={Object.values(d.missing.reduce((acc, r) => { acc[r.attribute] = acc[r.attribute] || { atributo: r.attribute, 'sin dato (máx. por año)': 0 }; acc[r.attribute]['sin dato (máx. por año)'] = Math.max(acc[r.attribute]['sin dato (máx. por año)'], r.missing_share); return acc }, {})).map(r => ({ ...r, 'sin dato (máx. por año)': percent(r['sin dato (máx. por año)']) }))} />
      </Card>
    </div>
  </FindingModule>
}
