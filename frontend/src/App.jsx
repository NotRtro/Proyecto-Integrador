import { useEffect, useState } from 'react'
import {
  Activity, BarChart3, Blocks, BookOpenCheck, Building2, DatabaseZap, FlaskConical, Menu, SearchCode, Users, X,
} from 'lucide-react'
import Overview from './pages/Overview'
import Data from './pages/Data'
import Eda from './pages/Eda'
import Concentration from './pages/Concentration'
import Competition from './pages/Competition'
import Similarity from './pages/Similarity'
import Lab from './pages/Lab'
import Discussion from './pages/Discussion'
import { SourceFooter } from './components'
import { useMeta } from './hooks'

// Navegación organizada por pregunta de interés; cada página indica qué Parte del enunciado cubre.
const pages = [
  ['inicio', 'Inicio', BarChart3, Overview, 'Preguntas y hallazgos'],
  ['datos', 'Datos y KDD', DatabaseZap, Data, 'Parte I'],
  ['eda', 'EDA y Spark', Activity, Eda, 'Parte I'],
  ['q1', 'Q1 · Concentración', Building2, Concentration, 'Partes I y IV'],
  ['q2', 'Q2 · Competencia', Users, Competition, 'Partes I, IV y V'],
  ['q3', 'Q3 · Requerimientos similares', SearchCode, Similarity, 'Partes II y III'],
  ['laboratorio', 'Laboratorio de técnicas', FlaskConical, Lab, 'Partes II a V'],
  ['discusion', 'Discusión', BookOpenCheck, Discussion, 'Parte VI'],
]
const byId = Object.fromEntries(pages.map(page => [page[0], page]))

export default function App() {
  const initial = window.location.hash.slice(1)
  const [page, setPage] = useState(byId[initial] ? initial : 'inicio')
  const [open, setOpen] = useState(false)
  const meta = useMeta()
  const Page = byId[page][3]
  useEffect(() => {
    const sync = () => { const target = window.location.hash.slice(1); if (byId[target]) setPage(target) }
    window.addEventListener('hashchange', sync)
    return () => window.removeEventListener('hashchange', sync)
  }, [])
  const navigate = id => { window.location.hash = id; setPage(id); setOpen(false); window.scrollTo({ top: 0, behavior: 'smooth' }) }
  return <div className="app-shell">
    <button className="menu-button" onClick={() => setOpen(!open)} aria-label="Abrir menú">{open ? <X /> : <Menu />}</button>
    <aside className={open ? 'open' : ''}>
      <div className="brand"><div className="brand-mark"><Blocks /></div><div><strong>Observatorio</strong><span>Contratación pública</span></div></div>
      <nav>{pages.map(([id, label, Icon, , part]) => <button className={page === id ? 'active' : ''} onClick={() => navigate(id)} key={id} title={part}><Icon size={19} /><span>{label}</span></button>)}</nav>
      <div className="sidebar-note"><span className="live-dot" />Datos procesados <strong>{meta.data?.period || '…'}</strong><small>Fuente: OECE · OCDS · CC BY 4.0</small></div>
    </aside>
    <main>
      <div className="topbar"><div><span className="live-dot" />Resultados precalculados con Spark</div><div>{byId[page][4]}</div></div>
      <Page navigate={navigate} />
      <SourceFooter meta={meta.data} />
    </main>
  </div>
}
