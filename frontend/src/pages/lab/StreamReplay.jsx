import { useEffect, useState } from 'react'
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { bytes, decimal, integer, percent } from '../../api'
import { Card, DataTable, Filters, FindingModule, Range, Stat } from '../../components'

// Reproducción del flujo semana a semana (Spark Structured Streaming, un micro-lote por semana).
export default function StreamReplay({ d }) {
  const batches = d.replay || []
  const [position, setPosition] = useState(batches.length)
  const [playing, setPlaying] = useState(false)
  useEffect(() => {
    if (!playing) return undefined
    const timer = setInterval(() => setPosition(p => {
      if (p >= batches.length) { setPlaying(false); return p }
      return p + 1
    }), 120)
    return () => clearInterval(timer)
  }, [playing, batches.length])
  if (!batches.length) return null
  const s = d.replay_summary
  const shown = batches.slice(0, Math.max(1, position))
  const current = shown.at(-1)
  const play = () => { if (position >= batches.length) setPosition(1); setPlaying(!playing) }
  return <FindingModule id="lab-flujo" part="Parte IV · simulación del flujo" question="¿Cómo evolucionan las estructuras de resumen a medida que llegan las adjudicaciones, semana a semana?"
    pipeline={[['Un Parquet por semana', 'local', 'mtime creciente = orden de llegada'], ['readStream (maxFilesPerTrigger=1)', 'Spark', 'Structured Streaming'], ['foreachBatch', 'Spark', 'un micro-lote = una semana'], ['Bloom + Count-Min + DGIM', 'local', 'y sus versiones exactas']]}>
    <Filters>
      <button className="play" onClick={play}>{playing ? 'Pausa' : position >= batches.length ? 'Reproducir desde el inicio' : 'Reproducir'}</button>
      <Range label="Micro-lote" value={position} onChange={v => { setPlaying(false); setPosition(v) }} min={1} max={batches.length} step={1} />
      <p className="hint">Semana {current.week_start} a {current.week_end} · {integer(current.events)} adjudicaciones en este micro-lote</p>
    </Filters>
    <div className="stat-grid four">
      <Stat label="Adjudicaciones recibidas" value={integer(current.cumulative_events)} note={`micro-lote ${current.batch + 1} de ${batches.length}`} />
      <Stat label="FPR del Bloom" value={percent(current.bloom_fpr_cumulative, 2)} note={`teórico con la ocupación actual: ${percent(current.bloom_fpr_theoretical, 2)}`} tone="orange" />
      <Stat label="Error de Count-Min" value={percent(current.cms_top10_relative_error, 1)} note="medio en los 10 proveedores más frecuentes" tone="green" />
      <Stat label="Postor único (30 días)" value={`${integer(current.dgim_exact)} · ${decimal(current.dgim_estimate, 0)}`} note="exacto · DGIM" tone="navy" />
    </div>
    <div className="two-col">
      <Card title="Postor único en los últimos 30 días" subtitle="Exacto frente a DGIM, al cierre de cada micro-lote">
        <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
          <LineChart data={shown} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="week_start" tick={{ fontSize: 10 }} minTickGap={40} /><YAxis tickFormatter={integer} />
            <Tooltip /><Legend /><Line dataKey="dgim_exact" name="Exacto" stroke="#384b72" dot={false} isAnimationActive={false} /><Line dataKey="dgim_estimate" name="DGIM" stroke="#de7c45" dot={false} isAnimationActive={false} />
          </LineChart></ResponsiveContainer></div>
      </Card>
      <Card title="El Bloom se va llenando" subtitle="FPR acumulado observado frente al teórico con la ocupación de ese momento">
        <div style={{ width: '100%', height: 260 }}><ResponsiveContainer>
          <LineChart data={shown} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="week_start" tick={{ fontSize: 10 }} minTickGap={40} /><YAxis tickFormatter={v => percent(v, 0)} />
            <Tooltip formatter={v => percent(v, 2)} /><Legend /><Line dataKey="bloom_fpr_theoretical" name="Teórico (ocupación actual)" stroke="#de7c45" dot={false} isAnimationActive={false} /><Line dataKey="bloom_fpr_batch" name="Observado en la semana" stroke="#167c80" dot={false} isAnimationActive={false} />
          </LineChart></ResponsiveContainer></div>
      </Card>
    </div>
    <DataTable rows={[{ motor: s.engine, 'micro-lotes': s.batches, adjudicaciones: s.events, 'tiempo total': `${decimal(s.wall_seconds, 1)} s`, 'por micro-lote (medio · p95)': `${decimal(s.mean_batch_seconds * 1000, 0)} · ${decimal(s.p95_batch_seconds * 1000, 0)} ms`, 'eventos por segundo': integer(s.events_per_second), 'memoria de las estructuras': bytes(s.state_bytes) }]} />
  </FindingModule>
}
