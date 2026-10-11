import { useState } from 'react'
import { decimal, integer, percent } from '../../api'
import { Card, DataTable, ExecutiveBar, Filters, FindingModule, Range, Select } from '../../components'

// Conjuntos frecuentes de FP-Growth (atributos del proceso) y canastas de segmentos UNSPSC.
export default function Itemsets({ d }) {
  const [size, setSize] = useState('2')
  const [support, setSupport] = useState(0.05)
  if (!d.itemsets?.length) return null
  const sets = d.itemsets.map(r => ({ ...r, items: JSON.parse(r.items) }))
    .filter(r => (size === 'Todos' || r.items.length === Number(size)) && r.support >= support)
  const co = d.copurchase_summary || {}
  return <FindingModule id="lab-itemsets" part="Parte V · conjuntos frecuentes" question="¿Qué características y qué bienes aparecen juntos con más frecuencia?"
    pipeline={[['Canastas de atributos', 'Spark', '196 mil procesos'], ['FP-Growth (soporte ≥ 2 %)', 'Spark', 'MLlib'], ['Canastas de segmentos UNSPSC', 'Spark', 'procesos con ítems de 2+ segmentos'], ['FP-Growth con mínimo de 30 procesos', 'Spark']]}>
    <Filters>
      <Select label="Tamaño del conjunto" value={size} onChange={setSize} options={['1', '2', '3', '4']} />
      <Range label="Soporte mínimo" value={support} onChange={setSupport} min={0.02} max={0.5} step={0.01} />
    </Filters>
    <div className="two-col">
      <Card title="Conjuntos frecuentes de atributos" subtitle={`${integer(sets.length)} conjuntos con los filtros (de los 60 con más soporte)`}>
        <ExecutiveBar data={sets.slice(0, 12).map(r => ({ ...r, conjunto: r.items.join(' + ') }))} category="conjunto" value="support" formatter={v => percent(v)} height={380} />
      </Card>
      <Card title="Qué se compra junto" subtitle={`${integer(co.multi_segment_baskets)} procesos (${percent(co.multi_segment_share)}) piden ítems de más de un segmento`}>
        <DataTable rows={(d.copurchase_itemsets || []).filter(r => r.size >= 2).map(r => ({ segmentos: r.names, procesos: r.count, soporte: percent(r.support) }))} limit={12} />
      </Card>
    </div>
    <DataTable rows={sets.map(r => ({ conjunto: r.items.join(' ∧ '), tamaño: r.items.length, procesos: r.frequency, soporte: percent(r.support) }))} limit={20} />
    {d.copurchase_rules?.length > 0 && <Card title="Reglas de compra conjunta" subtitle="Si un proceso pide ítems del segmento A, ¿qué otro segmento pide? (sobre los procesos con varios segmentos)">
      <DataTable rows={d.copurchase_rules.map(r => ({ si: r.antecedent, entonces: r.consequent, soporte: percent(r.support), confianza: percent(r.confidence), lift: decimal(r.lift) }))} limit={15} />
    </Card>}
  </FindingModule>
}
