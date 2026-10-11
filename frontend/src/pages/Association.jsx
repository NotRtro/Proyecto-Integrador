import { useState } from 'react'
import { api, integer, percent } from '../api'
import { Card, DataTable, ErrorState, ExecutiveBar, Filters, Loading, Range, SectionHeader, Select, Stat, Technical } from '../components'
import { useAsync, useMeta } from '../hooks'

export default function Association() {
  const [filters, setFilters] = useState({ year: 'Todos', support: .02, confidence: .5, lift: 1.01, size: 4, limit: 50 })
  const set = (key, value) => setFilters(current => ({ ...current, [key]: value }))
  const meta = useMeta()
  const { data, loading, error } = useAsync(() => api('/api/association', filters), Object.values(filters))
  if (error || meta.error) return <ErrorState error={error || meta.error} />
  if (loading || meta.loading) return <Loading />
  const top = data.rules.slice(0, 10).map((r, i) => ({ ...r, label: `R${i + 1}` }))
  return <>
    <SectionHeader eyebrow="Pregunta 5" title="Patrones que aparecen juntos"
      description="Las reglas frecuentes condensan combinaciones de categoría, método, territorio, monto y competencia."
      question="¿Qué características de contratación aparecen juntas con mayor frecuencia?" />
    <Filters><Select label="Cobertura" value={filters.year} onChange={v => set('year', v)} options={meta.data.years.map(String)} /><Range label="Soporte" value={filters.support} onChange={v => set('support', v)} min={.02} max={.2} step={.01} /><Range label="Confianza" value={filters.confidence} onChange={v => set('confidence', v)} min={.2} max={1} step={.05} /><Range label="Lift" value={filters.lift} onChange={v => set('lift', v)} min={1} max={5} step={.1} /></Filters>
    <div className="stat-grid three"><Stat label="Reglas visibles" value={integer(data.total_rules)} /><Stat label="Reglas interpretadas" value={data.selected.found} tone="green" /><Stat label="Estado" value={data.selected.status === 'complete' ? 'Completo' : 'Insuficiente'} tone="orange" /></div>
    <div className="two-col"><Card title="Reglas principales" subtitle="Lift de las primeras diez reglas"><ExecutiveBar data={top} category="label" value="lift" formatter={v => Number(v).toFixed(2)} /></Card><Card title="Cómo leer una regla" subtitle="Ejemplo bajo los filtros actuales"><div className="rule-anatomy">{data.rules[0] ? <><span>{data.rules[0].antecedent}</span><b>→</b><span>{data.rules[0].consequent}</span><dl><div><dt>Soporte</dt><dd>{percent(data.rules[0].support)}</dd></div><div><dt>Confianza</dt><dd>{percent(data.rules[0].confidence)}</dd></div><div><dt>Lift</dt><dd>{Number(data.rules[0].lift).toFixed(2)}</dd></div></dl></> : <p>No hay reglas para estos umbrales.</p>}</div></Card></div>
    <Card title="Ocho hallazgos interpretados" subtitle="Selección global no redundante">{data.selected.rules.map((rule, i) => <div className="selected-rule" key={i}><span>{String(i + 1).padStart(2, '0')}</span><p>{rule.interpretation}</p></div>)}</Card>
    <div className="callout"><strong>Asociación no es causalidad</strong><p>Una regla describe coexistencia en el conjunto publicado. No prueba influencia, colusión ni irregularidad. Los rangos discretizados y la composición territorial afectan el resultado.</p></div>
    <Technical title="Detalle técnico: reglas, Apriori y FP-Growth">
      <h4>Reglas filtradas</h4><DataTable rows={data.rules} />
      <h4>Benchmark de algoritmos</h4><DataTable rows={data.benchmark} />
      <p>Apriori propio y FP-Growth de Spark produjeron los mismos itemsets sobre la misma muestra. FP-Growth también se ejecutó sobre el conjunto completo.</p>
    </Technical>
  </>
}
