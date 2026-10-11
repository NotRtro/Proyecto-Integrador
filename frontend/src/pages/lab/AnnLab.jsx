import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from 'recharts'
import { api, bytes, decimal, integer, ms, percent } from '../../api'
import { Card, DataTable, ErrorState, FindingModule, Loading } from '../../components'
import { useAsync } from '../../hooks'

// Diagrama de HNSW: capas de un grafo; la búsqueda entra arriba (pocos nodos, saltos largos)
// y baja refinando hasta la capa 0, que contiene todos los vectores.
function HnswDiagram() {
  const layers = [
    { y: 40, nodes: [[90, 0], [330, 0]] },
    { y: 120, nodes: [[60, 0], [150, 0], [260, 0], [330, 0], [400, 0]] },
    { y: 200, nodes: [[40, 0], [80, 0], [120, 0], [150, 0], [190, 0], [230, 0], [260, 0], [300, 0], [330, 0], [370, 0], [400, 0], [440, 0]] },
  ]
  const path = [[90, 40], [330, 40], [330, 120], [260, 120], [260, 200], [230, 200]]
  return <svg viewBox="0 0 480 240" role="img" aria-label="Capas del grafo HNSW y recorrido de una búsqueda">
    {layers.map((layer, l) => <g key={l}>
      <text x="6" y={layer.y + 4} fontSize="10" fill="#667680">capa {2 - l}</text>
      {layer.nodes.slice(1).map(([x], i) => <line key={i} x1={layer.nodes[i][0]} y1={layer.y} x2={x} y2={layer.y} stroke="#c9d6d4" />)}
      {layer.nodes.map(([x], i) => <circle key={i} cx={x} cy={layer.y} r="6" fill="#d8ede7" stroke="#167c80" />)}
    </g>)}
    <polyline points={path.map(p => p.join(',')).join(' ')} fill="none" stroke="#de7c45" strokeWidth="2.5" strokeDasharray="5 3" />
    <circle cx="230" cy="200" r="8" fill="none" stroke="#de7c45" strokeWidth="2.5" />
    <text x="250" y="230" fontSize="10" fill="#a14a17">vecino encontrado</text>
  </svg>
}

// Diagrama de IVF: k-means divide el espacio en celdas; la consulta solo revisa las nprobe más cercanas.
function IvfDiagram() {
  const cells = [[80, 60], [210, 50], [350, 70], [120, 170], [260, 160], [400, 180]]
  const probed = [4, 1]
  const points = Array.from({ length: 60 }, (_, i) => {
    const [cx, cy] = cells[i % cells.length]
    return [cx + ((i * 37) % 70) - 35, cy + ((i * 53) % 60) - 30, i % cells.length]
  })
  return <svg viewBox="0 0 480 240" role="img" aria-label="Celdas de IVF y celdas exploradas por una consulta">
    {cells.map(([x, y], i) => <circle key={i} cx={x} cy={y} r="52" fill={probed.includes(i) ? '#fde7d8' : '#f3f7f5'} stroke="#c9d6d4" />)}
    {points.map(([x, y, c], i) => <circle key={i} cx={x} cy={y} r="3" fill={probed.includes(c) ? '#de7c45' : '#9fb2b2'} />)}
    {cells.map(([x, y], i) => <rect key={i} x={x - 5} y={y - 5} width="10" height="10" fill="#167c80" />)}
    <circle cx="240" cy="125" r="7" fill="#384b72" /><text x="252" y="129" fontSize="11" fill="#384b72">consulta (nprobe = 2)</text>
  </svg>
}

export default function AnnLab() {
  const result = useAsync(() => api('/api/ann'), [])
  if (result.error) return <ErrorState error={result.error} />
  if (result.loading) return <Loading />
  const { hnsw, ivf, scaling, methodology } = result.data
  const v = methodology.vectors || {}
  const agreement = methodology.part2_agreement
  return <FindingModule id="lab-ann" part="Parte III · IVF y HNSW" question="¿Cómo funcionan HNSW e IVF, y qué recall se pierde a cambio de su velocidad?"
    pipeline={[['TF-IDF + SVD', 'local', `${integer(v.documents)} vectores de ${v.dimensions} dimensiones`], ['Índices', 'local', 'faiss: HNSWFlat, IVFFlat'], ['200 consultas', 'local', 'un hilo, sin la propia consulta'], ['Recall@10 e IC 95 %', 'local', 'frente a fuerza bruta']]}>
    <div className="concept">
      <div><HnswDiagram />
        <p><strong>HNSW</strong> construye un grafo por capas: cada vector se conecta con sus <b>M</b> vecinos más cercanos y aparece en la capa superior con probabilidad decreciente. La búsqueda entra por la capa más alta (pocos nodos, saltos largos) y baja refinando hasta la capa 0. <b>efConstruction</b> fija cuántos candidatos se exploran al insertar (mejor grafo, construcción más lenta) y <b>efSearch</b> cuántos al consultar (más recall, más latencia).</p></div>
      <div><IvfDiagram />
        <p><strong>IVF</strong> agrupa los vectores con k-means en <b>nlist</b> celdas y guarda cada uno en la lista de su centroide. Una consulta solo revisa las <b>nprobe</b> celdas con centroide más cercano: con nprobe pequeño es muy rápida pero puede perder vecinos que cayeron en una celda vecina; con nprobe = nlist equivale a la búsqueda exacta.</p></div>
    </div>
    <Card title="Recall frente a latencia" subtitle="Cada punto es una configuración; arriba a la izquierda es mejor">
      <div style={{ width: '100%', height: 320 }}><ResponsiveContainer>
        <ScatterChart margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" />
          <XAxis type="number" dataKey="mean_ms" name="Latencia (ms)" scale="log" domain={['auto', 'auto']} tickFormatter={v => decimal(v, 2)} />
          <YAxis type="number" dataKey="recall_at_10" name="Recall@10" domain={[0.7, 1]} tickFormatter={v => percent(v, 0)} />
          <Tooltip formatter={(value, name) => name === 'Recall@10' ? percent(value) : ms(value)} labelFormatter={() => ''} /><Legend />
          <Scatter name="HNSW" data={hnsw} fill="#167c80" /><Scatter name="IVF" data={ivf} fill="#de7c45" />
        </ScatterChart></ResponsiveContainer></div>
    </Card>
    <Card title="Escalamiento" subtitle="La búsqueda exacta crece con el tamaño del corpus; HNSW casi no">
      <div style={{ width: '100%', height: 280 }}><ResponsiveContainer>
        <LineChart data={scaling} margin={{ left: 8, right: 16 }}><CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="corpus_size" type="number" scale="log" domain={['auto', 'auto']} tickFormatter={integer} />
          <YAxis scale="log" domain={['auto', 'auto']} tickFormatter={v => decimal(v, 2)} /><Tooltip formatter={v => ms(v)} labelFormatter={integer} /><Legend />
          <Line dataKey="exact_mean_ms" name="Exacta" stroke="#384b72" strokeWidth={2.5} /><Line dataKey="hnsw_mean_ms" name="HNSW" stroke="#167c80" strokeWidth={2.5} /><Line dataKey="ivf_mean_ms" name="IVF" stroke="#de7c45" strokeWidth={2.5} />
        </LineChart></ResponsiveContainer></div>
    </Card>
    <DataTable rows={scaling.map(r => ({ procesos: r.corpus_size, 'exacta (ms)': decimal(r.exact_mean_ms, 3), 'HNSW (ms)': decimal(r.hnsw_mean_ms, 3), 'recall HNSW': percent(r.hnsw_recall_at_10), 'construcción HNSW (s)': decimal(r.hnsw_build_seconds, 1), 'IVF (ms)': decimal(r.ivf_mean_ms, 3), 'recall IVF': percent(r.ivf_recall_at_10), 'construcción IVF (s)': decimal(r.ivf_build_seconds, 1), 'matriz exacta': bytes(r.exact_matrix_mb * 1e6) }))} />
    <div className="callout positive"><strong>Representación</strong><p>TF-IDF de 1 y 2 palabras ({integer(v.vocabulary)} términos) reducido con SVD a {v.dimensions} dimensiones, que conservan {percent(v.explained_variance)} de la varianza. {agreement && `De ${agreement.pairs} pares casi idénticos de la Parte II, ${percent(agreement.in_top10)} aparecen entre los 10 vecinos por coseno (contando empates): ambas representaciones coinciden en lo esencial.`}</p></div>
    <DataTable rows={[...hnsw, ...ivf].map(r => ({ configuración: r.config, 'recall@10': percent(r.recall_at_10), 'IC 95 %': `${percent(r.recall_at_10_ci_low)} – ${percent(r.recall_at_10_ci_high)}`, 'latencia media': ms(r.mean_ms), p95: ms(r.p95_ms), aceleración: `${decimal(r.speedup, 0)}×`, 'construcción (s)': decimal(r.build_seconds, 1), índice: bytes(r.index_bytes) }))} />
  </FindingModule>
}
