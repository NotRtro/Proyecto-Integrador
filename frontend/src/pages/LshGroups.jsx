import { useState } from 'react'
import { decimal, integer, money, percent } from '../api'
import { Card, DataTable, ExecutiveBar, Filters, FindingModule, Range, Select, Stat } from '../components'

const SCOPES = { todos: 'Todos los grupos', una: 'Una sola entidad', varias: 'Varias entidades' }

// Grupos de requerimientos casi idénticos (componentes conexas de los pares LSH) con su etiqueta.
export default function LshGroups({ s }) {
  const [minSize, setMinSize] = useState(3)
  const [scope, setScope] = useState('todos')
  if (!s.groups?.length) return null
  const g = s.groups_summary
  const rows = s.groups.filter(r => r.processes >= minSize
    && (scope === 'todos' || (scope === 'una' ? r.buyers === 1 : r.buyers > 1)))
  return <FindingModule id="q3-grupos" part="Parte II · grupos y etiquetas de LSH" question="¿Qué requerimientos se repiten, agrupados por tema?"
    pipeline={[['Pares con Jaccard ≥ 0,7', 'Spark', 'sin pares del mismo procedimiento'], ['Componentes conexas', 'local', 'union-find'], ['Frecuencia documental del corpus', 'Spark'], ['Etiqueta = TF-IDF del grupo', 'local', 'palabras más distintivas']]}>
    <div className="stat-grid four">
      <Stat label="Grupos (3+ procesos)" value={integer(g.groups)} note={`${integer(g.processes_in_groups)} procesos`} />
      <Stat label="Una sola entidad" value={integer(g.single_buyer_groups)} note="repetición interna" tone="orange" />
      <Stat label="Varias entidades" value={integer(g.multi_buyer_groups)} note="requerimientos estandarizables" tone="green" />
      <Stat label="Grupo más grande" value={integer(g.largest)} note={`mediana ${decimal(g.median_size, 0)} procesos`} tone="navy" />
    </div>
    <Filters>
      <Range label="Tamaño mínimo" value={minSize} onChange={setMinSize} min={3} max={100} step={1} />
      <Select label="Entidades" value={scope} onChange={setScope} options={Object.keys(SCOPES)} all={false} format={v => SCOPES[v]} />
    </Filters>
    <Card title="Grupos más grandes y su etiqueta" subtitle={`${integer(rows.length)} grupos con los filtros actuales (se muestran los 200 más grandes del corpus)`}>
      <ExecutiveBar data={rows.slice(0, 15).map(r => ({ ...r, etiqueta: r.label || `grupo ${r.group}` }))} category="etiqueta" value="processes" formatter={integer} height={380} />
    </Card>
    <DataTable rows={rows.map(r => ({ grupo: r.group, etiqueta: r.label, procesos: r.processes, entidades: r.buyers, 'entidad principal': r.top_buyer, 'su peso': percent(r.top_buyer_share), categoría: r.category, modalidad: r.modality, años: r.years, 'monto convocado': money(r.tender_amount_pen), 'Jaccard medio': decimal(r.mean_jaccard, 2) }))} />
  </FindingModule>
}
