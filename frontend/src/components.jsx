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

// ---------------------------------------------------------------------------
// Anatomía de cada hallazgo (enunciado): pregunta → pipeline → visualización →
// controles → interpretación escrita. La interpretación se lee de
// src/content/hallazgos/<id>.md, que escribe el equipo.

const narratives = import.meta.glob('./content/hallazgos/*.md', { query: '?raw', import: 'default', eager: true })

export function narrative(id) {
  return narratives[`./content/hallazgos/${id}.md`] || ''
}

function inline(text) {
  return text.split(/(\*\*[^*]+\*\*)/g).map((part, i) =>
    part.startsWith('**') && part.endsWith('**') ? <strong key={i}>{part.slice(2, -2)}</strong> : part)
}

// Markdown mínimo: párrafos, listas con "- ", subtítulos "### " y **negritas**.
export function Markdown({ text }) {
  const blocks = text.trim().split(/\n\s*\n/)
  return <div className="markdown">{blocks.map((block, i) => {
    const lines = block.split('\n')
    if (lines.every(line => line.trim().startsWith('- '))) {
      return <ul key={i}>{lines.map((line, j) => <li key={j}>{inline(line.trim().slice(2))}</li>)}</ul>
    }
    if (block.startsWith('### ')) return <h4 key={i}>{inline(block.slice(4))}</h4>
    return <p key={i}>{inline(block.replace(/\n/g, ' '))}</p>
  })}</div>
}

export function Interpretation({ id }) {
  const text = narrative(id)
  const pending = !text.trim() || text.includes('PENDIENTE')
  return <div className={`interpretation ${pending ? 'pending' : ''}`}>
    <span>Interpretación del equipo</span>
    {pending
      ? <><p>Pendiente de redacción en <code>frontend/src/content/hallazgos/{id}.md</code>.</p>
        <Markdown text={text.split('\n').filter(line => !line.startsWith('PENDIENTE')).join('\n')} /></>
      : <Markdown text={text} />}
  </div>
}

export function Pipeline({ steps }) {
  return <ol className="pipeline">{steps.map(([label, engine, detail], i) => <li key={i}>
    <span className={`engine ${engine === 'Spark' ? 'spark' : 'local'}`}>{engine}</span>
    <strong>{label}</strong>{detail && <small>{detail}</small>}
  </li>)}</ol>
}

export function FindingModule({ id, part, question, pipeline, children }) {
  return <section className="finding" id={id}>
    <div className="finding-head"><span className="part-tag">{part}</span><h2>{question}</h2></div>
    {pipeline && <Pipeline steps={pipeline} />}
    {children}
    <Interpretation id={id} />
  </section>
}

export function CodeCompare({ snippet }) {
  if (!snippet) return null
  return <div className="code-compare">{['DataFrame', 'RDD'].map(engine => snippet[engine] && <div key={engine}>
    <b>{engine} · {snippet[engine].lines} líneas</b><pre>{snippet[engine].code}</pre>
  </div>)}</div>
}

export function SourceFooter({ meta }) {
  const source = meta?.source || {}
  return <footer className="source-footer">
    Fuente: Registro de Datos OCDS del Organismo Especializado para las Contrataciones Públicas Eficientes (OECE), {' '}
    <a href={source.page || 'https://data.open-contracting.org/es/publication/135'} target="_blank" rel="noreferrer">data.open-contracting.org/es/publication/135</a>.
    {' '}Licencia {source.license || 'CC BY 4.0'}.{source.published_at && ` Publicación del ${source.published_at}`}{source.downloaded_at && `, descargada el ${source.downloaded_at.slice(0, 10)}`}.
    {' '}Las cifras son señales exploratorias: ninguna similitud, concentración o regla prueba por sí sola una irregularidad.
  </footer>
}

export function StaticNotice({ data }) {
  if (!data?.static_ignored?.length) return null
  return <div className="callout"><strong>Versión publicada</strong><p>Los filtros {data.static_ignored.join(', ')} solo funcionan en la versión local con API; se muestran los resultados sin ellos.</p></div>
}
