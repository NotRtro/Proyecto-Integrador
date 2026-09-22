import { ChevronDown, CircleAlert, Database, LoaderCircle } from 'lucide-react'
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer,
  Tooltip, XAxis, YAxis,
} from 'recharts'

export function Loading() {
  return <div className="state"><LoaderCircle className="spin" /><p>Preparando resultados…</p></div>
}

export function ErrorState({ error }) {
  const missing = error.message.includes('Artefacto no generado')
  return <div className="state error"><CircleAlert /><div><strong>No pudimos cargar esta sección</strong><p>{error.message}</p>{missing && <><p>Genera los resultados con:</p><code>docker compose run --rm pipeline python -m src.run_all</code></>}</div></div>
}

export function SectionHeader({ eyebrow, title, description, question }) {
  return <header className="section-header">
    <span className="eyebrow">{eyebrow}</span>
    <h1>{title}</h1>
    <p>{description}</p>
    {question && <div className="question"><span>Pregunta que responde</span>{question}</div>}
  </header>
}

export function Stat({ label, value, note, tone = 'blue' }) {
  return <article className={`stat ${tone}`}><span>{label}</span><strong>{value}</strong>{note && <small>{note}</small>}</article>
}

export function Card({ title, subtitle, children, className = '' }) {
  return <article className={`card ${className}`}><div className="card-head"><div><h3>{title}</h3>{subtitle && <p>{subtitle}</p>}</div></div>{children}</article>
}

export function Technical({ title = 'Ver detalle técnico', children, defaultOpen = false }) {
  return <details className="technical" open={defaultOpen}>
    <summary><span><Database size={17} />{title}</span><ChevronDown size={18} /></summary>
    <div className="technical-body">{children}</div>
  </details>
}

const tooltipStyle = { borderRadius: 12, border: '1px solid #dbe4ee', boxShadow: '0 10px 24px rgba(17, 34, 51, .1)' }

export function ExecutiveBar({ data, category, value, color = '#167c80', formatter = (v) => v, height = 300 }) {
  return <div style={{ width: '100%', height }}><ResponsiveContainer>
    <BarChart data={data} layout="vertical" margin={{ top: 8, right: 24, bottom: 8, left: 20 }}>
      <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e7edf2" />
      <XAxis type="number" tickFormatter={formatter} tick={{ fontSize: 11 }} />
      <YAxis dataKey={category} type="category" width={120} tick={{ fontSize: 11 }} tickFormatter={(v) => String(v || 'Sin dato').slice(0, 20)} />
      <Tooltip formatter={formatter} contentStyle={tooltipStyle} />
      <Bar dataKey={value} fill={color} radius={[0, 5, 5, 0]} />
    </BarChart>
  </ResponsiveContainer></div>
}

export function ExecutiveLine({ data, x, lines, height = 300, formatter = (v) => v }) {
  const palette = ['#167c80', '#de7c45', '#384b72']
  return <div style={{ width: '100%', height }}><ResponsiveContainer>
    <LineChart data={data} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
      <CartesianGrid strokeDasharray="3 3" stroke="#e7edf2" />
      <XAxis dataKey={x} tick={{ fontSize: 11 }} />
      <YAxis tickFormatter={formatter} tick={{ fontSize: 11 }} />
      <Tooltip formatter={formatter} contentStyle={tooltipStyle} />
      {lines.map((line, index) => <Line key={line} type="monotone" dataKey={line} stroke={palette[index % palette.length]} strokeWidth={2.5} dot={false} />)}
    </LineChart>
  </ResponsiveContainer></div>
}

export function DataTable({ rows, columns, limit = 25 }) {
  if (!rows?.length) return <p className="empty">No hay resultados con estos filtros.</p>
  const cols = columns || Object.keys(rows[0])
  return <div className="table-wrap"><table><thead><tr>{cols.map(c => <th key={c}>{c.replaceAll('_', ' ')}</th>)}</tr></thead>
    <tbody>{rows.slice(0, limit).map((row, i) => <tr key={i}>{cols.map(c => <td key={c}>{formatCell(row[c])}</td>)}</tr>)}</tbody></table></div>
}

function formatCell(value) {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Sí' : 'No'
  if (typeof value === 'number') return Number.isInteger(value) ? value.toLocaleString('es-PE') : value.toLocaleString('es-PE', { maximumFractionDigits: 4 })
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

export function Filters({ children }) { return <div className="filters">{children}</div> }
export function Select({ label, value, onChange, options, all = true, format = value => value }) {
  return <label><span>{label}</span><select value={value} onChange={e => onChange(e.target.value)}>
    {all && <option value="Todos">Todos</option>}{options.map(option => <option key={option} value={option}>{format(option)}</option>)}
  </select></label>
}
export function Range({ label, value, onChange, min, max, step }) {
  return <label className="range"><span>{label}<b>{value}</b></span><input type="range" value={value} onChange={e => onChange(Number(e.target.value))} min={min} max={max} step={step} /></label>
}
