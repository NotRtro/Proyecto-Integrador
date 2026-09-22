import { useEffect, useState } from 'react'
import {
  Activity, BarChart3, Blocks, BookOpenCheck, BrainCircuit, Building2,
  DatabaseZap, Menu, Network, SearchCode, X,
} from 'lucide-react'
import Overview from './pages/Overview'
import Quality from './pages/Quality'
import Concentration from './pages/Concentration'
import Similarity from './pages/Similarity'
import Ann from './pages/Ann'
import Streaming from './pages/Streaming'
import Association from './pages/Association'
import Discussion from './pages/Discussion'

const pages = [
  ['overview', 'Resumen ejecutivo', BarChart3],
  ['quality', 'Datos y calidad', DatabaseZap],
  ['concentration', 'Concentración', Building2],
  ['similarity', 'Similitud LSH', SearchCode],
  ['ann', 'Vecinos ANN', BrainCircuit],
  ['streaming', 'Flujos', Activity],
  ['association', 'Asociaciones', Network],
  ['discussion', 'Discusión', BookOpenCheck],
]

const content = {
  overview: Overview, quality: Quality, concentration: Concentration,
  similarity: Similarity, ann: Ann, streaming: Streaming,
  association: Association, discussion: Discussion,
}

export default function App() {
  const initialPage = window.location.hash.slice(1)
  const [page, setPage] = useState(content[initialPage] ? initialPage : 'overview')
  const [open, setOpen] = useState(false)
  const Page = content[page]
  useEffect(() => {
    const sync = () => { const target = window.location.hash.slice(1); if (content[target]) setPage(target) }
    window.addEventListener('hashchange', sync)
    return () => window.removeEventListener('hashchange', sync)
  }, [])
  const navigate = id => { window.location.hash = id; setPage(id); setOpen(false); window.scrollTo({ top: 0, behavior: 'smooth' }) }
  return <div className="app-shell">
    <button className="menu-button" onClick={() => setOpen(!open)} aria-label="Abrir menú">{open ? <X /> : <Menu />}</button>
    <aside className={open ? 'open' : ''}>
      <div className="brand"><div className="brand-mark"><Blocks /></div><div><strong>Observatorio</strong><span>Contratación pública</span></div></div>
      <nav>{pages.map(([id, label, Icon]) => <button className={page === id ? 'active' : ''} onClick={() => navigate(id)} key={id}><Icon size={19} /><span>{label}</span></button>)}</nav>
      <div className="sidebar-note"><span className="live-dot" />Datos procesados <strong>2023–2025</strong><small>Fuente: OECE · OCDS</small></div>
    </aside>
    <main><div className="topbar"><div><span className="live-dot" />Resultados precalculados</div><span>Vista ejecutiva + detalle técnico</span></div><Page /></main>
  </div>
}
