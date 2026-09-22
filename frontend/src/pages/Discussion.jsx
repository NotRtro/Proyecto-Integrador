import { api, ms, percent } from '../api'
import { Card, DataTable, ErrorState, Loading, SectionHeader, Technical } from '../components'
import { useAsync } from '../hooks'

export default function Discussion() {
  const overview = useAsync(() => api('/api/overview'), [])
  const streaming = useAsync(() => api('/api/streaming'), [])
  if (overview.loading || streaming.loading) return <Loading />
  if (overview.error || streaming.error) return <ErrorState error={overview.error || streaming.error} />
  const o = overview.data; const a = o.best_ann; const l = o.best_lsh
  const matrix = [
    { técnica: 'Jaccard exacto', exactitud: 'Referencia', tiempo: `${Number(l.exact_seconds).toFixed(3)} s`, escalabilidad: 'Baja', valor: 'Verdad de referencia' },
    { técnica: 'MinHash LSH', exactitud: `F1 ${Number(l.f1).toFixed(3)}`, tiempo: `${Number(l.lsh_seconds).toFixed(3)} s`, escalabilidad: 'Alta', valor: 'Reducir pares' },
    { técnica: 'Coseno exacto', exactitud: 'Referencia', tiempo: ms(a.exact_mean_ms), escalabilidad: 'Lineal', valor: 'Vecinos exactos' },
    { técnica: 'HNSW', exactitud: `Recall ${percent(a.recall_at_10)}`, tiempo: ms(a.mean_ms), escalabilidad: 'Alta en RAM', valor: 'Consulta rápida' },
    { técnica: 'Bloom', exactitud: 'Sin falsos negativos', tiempo: 'Microsegundos', escalabilidad: 'Flujo', valor: 'Pertenencia' },
    { técnica: 'CMS', exactitud: 'Sin subestimación', tiempo: 'Microsegundos', escalabilidad: 'Flujo', valor: 'Frecuencias' },
    { técnica: 'DGIM', exactitud: 'Aproximada', tiempo: 'Microsegundos', escalabilidad: 'Flujo', valor: 'Ventana reciente' },
  ]
  return <>
    <SectionHeader eyebrow="Cierre" title="Qué aporta cada técnica y dónde termina la evidencia"
      description="Una comparación crítica separa los resultados observados de las decisiones metodológicas y sus límites."
      question="¿La reducción de tiempo o memoria justifica la pérdida de precisión?" />
    <Card title="Matriz de decisión" subtitle="Exactitud, desempeño, escalabilidad y valor"><DataTable rows={matrix} /></Card>
    <div className="two-col">
      <Card title="Mayor valor obtenido"><div className="narrative"><p><strong>EDA y concentración</strong> responden directamente cómo se distribuyen procesos y montos.</p><p><strong>LSH y HNSW</strong> ayudan a explorar textos cuando comparar todo con todo deja de ser viable.</p><p><strong>FP-Growth</strong> resume patrones frecuentes, siempre como asociaciones descriptivas.</p></div></Card>
      <Card title="Valor condicionado"><div className="narrative"><p><strong>CMS</strong> aporta poco cuando solo se cuentan unas pocas categorías.</p><p><strong>ANN</strong> debe justificar el costo de construir el índice; en corpus pequeños puede ganar el método exacto.</p><p><strong>DGIM</strong> responde ventanas de eventos, no tendencias por tiempo calendario.</p></div></Card>
    </div>
    <div className="risk-grid"><div><span>01</span><p><strong>Los montos no son gasto ejecutado.</strong> Se analizan valores convocados y adjudicados publicados.</p></div><div><span>02</span><p><strong>La similitud no prueba duplicidad.</strong> Los textos pueden compartir plantillas o contextos legítimos.</p></div><div><span>03</span><p><strong>La asociación no prueba causalidad.</strong> El lift depende de la composición del conjunto.</p></div><div><span>04</span><p><strong>La geografía es institucional.</strong> El departamento corresponde al comprador, no necesariamente a la ejecución.</p></div></div>
    <Technical title="Detalle técnico: validaciones y trabajo pendiente">
      <DataTable rows={Object.entries(o.validation.checks).map(([control, result]) => ({ control, result }))} />
      <p>Con 10× datos se deben aumentar particiones y medir el driver. Con 100× conviene evitar recolectar candidatos e itemsets, particionar índices ANN y paginar agregados.</p>
    </Technical>
  </>
}
