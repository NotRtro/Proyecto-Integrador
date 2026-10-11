import { useState } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { integer, money, percent } from '../api'
import { Card, DataTable, Filters, FindingModule, Select, Stat } from '../components'

const RANKINGS = { mas_participan: 'Los que más participan', mas_ganan: 'Los que más ganan', nunca_ganan: 'Participan mucho y nunca ganan' }
const TYPES = { persona_juridica: 'Empresa', consorcio: 'Consorcio', extranjero: 'Extranjero', otro: 'Otro', persona_natural: 'Persona natural' }

// Quién participa, quién gana y quién nunca gana (tender.tenderers frente a las adjudicaciones).
export default function Participation({ d }) {
  const [ranking, setRanking] = useState('mas_participan')
  if (!d.participation?.length) return null
  const s = d.participation_summary
  const rows = d.participation.filter(r => r.ranking === ranking)
  const chart = rows.slice(0, 15).map(r => ({ ...r, label: String(r.name || r.id).slice(0, 28), perdidas: r.participations - r.wins }))
  const bins = d.participation_bins.map(r => ({ ...r, ganan: r.providers - r.never_won }))
  return <FindingModule id="q2-participacion" part="Parte I · participación" question="¿Quiénes participan más, quiénes ganan más y quiénes participan una y otra vez sin ganar nunca?"
    pipeline={[['tender.tenderers', 'Spark', 'participantes registrados por proceso'], ['suppliers', 'Spark', 'adjudicados'], ['Join por identificador', 'Spark', 'participaciones, victorias, tasa'], ['Rankings sin personas naturales', 'Spark']]}>
    <div className="stat-grid four">
      <Stat label="Proveedores que participan" value={integer(s.providers)} note={`en ${integer(s.processes_with_tenderers)} procesos`} />
      <Stat label="Nunca ganaron" value={percent(s.never_won_share)} note={`${integer(s.never_won)} proveedores`} tone="orange" />
      <Stat label="Tasa de victoria global" value={percent(s.overall_win_rate)} note="victorias / participaciones" tone="green" />
      <Stat label={`Con ${s.never_won_min}+ participaciones y 0 victorias`} value={integer(s.never_won_frequent)} tone="navy" />
    </div>
    <Filters><Select label="Ranking" value={ranking} onChange={setRanking} options={Object.keys(RANKINGS)} all={false} format={v => RANKINGS[v]} /></Filters>
    <div className="two-col">
      <Card title={RANKINGS[ranking]} subtitle="Participaciones divididas en ganadas y no ganadas (personas naturales excluidas)">
        <div style={{ width: '100%', height: 420 }}><ResponsiveContainer>
          <BarChart data={chart} layout="vertical" margin={{ left: 20, right: 16 }}>
            <CartesianGrid strokeDasharray="3 3" horizontal={false} /><XAxis type="number" tickFormatter={integer} />
            <YAxis dataKey="label" type="category" width={170} tick={{ fontSize: 10 }} />
            <Tooltip formatter={integer} /><Legend />
            <Bar dataKey="wins" name="Ganó" stackId="p" fill="#167c80" />
            <Bar dataKey="perdidas" name="No ganó" stackId="p" fill="#de7c45" />
          </BarChart></ResponsiveContainer></div>
      </Card>
      <Card title="Proveedores según cuántas veces participan" subtitle="Cuántos ganaron al menos una vez y cuántos nunca">
        <div style={{ width: '100%', height: 420 }}><ResponsiveContainer>
          <BarChart data={bins} margin={{ left: 8, right: 16 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} /><XAxis dataKey="bin" /><YAxis tickFormatter={integer} />
            <Tooltip formatter={integer} /><Legend />
            <Bar dataKey="ganan" name="Ganó alguna vez" stackId="b" fill="#167c80" />
            <Bar dataKey="never_won" name="Nunca ganó" stackId="b" fill="#de7c45" />
          </BarChart></ResponsiveContainer></div>
      </Card>
    </div>
    <DataTable rows={rows.map(r => ({ proveedor: r.name, tipo: TYPES[r.type] || r.type, participaciones: r.participations, victorias: r.wins, 'tasa de victoria': percent(r.win_rate), 'monto ganado': money(r.amount_won_pen), entidades: r.buyers_entered }))} />
    <DataTable rows={bins.map(r => ({ participaciones: r.bin, proveedores: r.providers, 'nunca ganaron': percent(r.never_won_share), 'tasa de victoria': percent(r.win_rate) }))} />
  </FindingModule>
}
