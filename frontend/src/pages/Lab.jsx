import { useState } from 'react'
import { SectionHeader } from '../components'
import AnnLab from './lab/AnnLab'
import RulesLab from './lab/RulesLab'
import SimilarityLab from './lab/SimilarityLab'
import StreamLab from './lab/StreamLab'

// Cada técnica con sus parámetros ajustables en vivo y sus compromisos de precisión, memoria y tiempo.
const TABS = [
  ['similitud', 'MinHash y LSH', SimilarityLab],
  ['vecinos', 'HNSW e IVF', AnnLab],
  ['flujos', 'Bloom, CMS y DGIM', StreamLab],
  ['reglas', 'Reglas de asociación', RulesLab],
]

export default function Lab() {
  const [tab, setTab] = useState('similitud')
  const Content = TABS.find(t => t[0] === tab)[2]
  return <>
    <SectionHeader eyebrow="Partes II a V · Laboratorio" title="Precisión, memoria y tiempo, técnica por técnica"
      description="Cada técnica aproximada se compara con su versión exacta sobre los mismos datos. Los controles recalculan en vivo las curvas teóricas y filtran los resultados medidos."
      question="¿Cuánto se pierde con los métodos aproximados frente a los exactos, y vale la pena?" />
    <div className="tabs">{TABS.map(([id, label]) => <button key={id} className={tab === id ? 'active' : ''} onClick={() => setTab(id)}>{label}</button>)}</div>
    <Content />
  </>
}
