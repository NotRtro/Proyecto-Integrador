import { api, bytes, decimal, integer, ms, percent } from '../api'
import { Card, DataTable, ErrorState, FindingModule, Loading, SectionHeader } from '../components'
import { useAsync } from '../hooks'

const ENGINE = { validation: 'local', processing: 'Spark', part1: 'Spark', part2: 'Spark + local', part3: 'local', part4: 'Spark + local', part5: 'Spark + local', export: 'local' }

// Ajuste lineal por mínimos cuadrados: latencia exacta (ms) en función de N.
function linearFit(points) {
  const n = points.length
  const mx = points.reduce((s, p) => s + p.x, 0) / n
  const my = points.reduce((s, p) => s + p.y, 0) / n
  const slope = points.reduce((s, p) => s + (p.x - mx) * (p.y - my), 0) / points.reduce((s, p) => s + (p.x - mx) ** 2, 0)
  return { slope, intercept: my - slope * mx }
}

export default function Discussion() {
  const disc = useAsync(() => api('/api/discussion'), [])
  const sim = useAsync(() => api('/api/similarity'), [])
  const ann = useAsync(() => api('/api/ann'), [])
  const stream = useAsync(() => api('/api/streaming'), [])
  const error = disc.error || sim.error || ann.error || stream.error
  if (error) return <ErrorState error={error} />
  if (disc.loading || sim.loading || ann.loading || stream.loading) return <Loading />
  const d = disc.data
  const corpus = sim.data.methodology.full_corpus
  const minhash128 = sim.data.minhash_error.find(r => r.hashes === 128)
  const bestLsh = [...sim.data.grid].filter(r => r.threshold === 0.7).sort((a, b) => b.f1 - a.f1)[0]
  const hnsw = ann.data.methodology.best_hnsw
  const ivf = ann.data.methodology.best_ivf
  const bloom = stream.data.bloom.find(r => r.bits === 524288 && r.hashes === 5) || stream.data.bloom[0]
  const cms = stream.data.cms.find(r => r.domain === 'proveedor' && r.width === 2048 && r.depth === 5) || stream.data.cms[0]
  const dgim = stream.data.dgim.find(r => r.signal === 'postor_unico' && r.unit === 'eventos' && r.window === 10000) || stream.data.dgim[0]
  const precision = [
    { técnica: 'MinHash (k = 128)', exacta: 'Jaccard exacto', pérdida: `RMSE ${decimal(minhash128?.rmse_similar, 3)} en pares similares`, ganancia: 'firma fija de 128 enteros por proceso' },
    { técnica: `LSH (b = ${bestLsh?.bands}, r = ${bestLsh?.rows})`, exacta: 'todos los pares', pérdida: `recall ${percent(bestLsh?.recall)} · precisión ${percent(bestLsh?.precision)}`, ganancia: `${integer(corpus.candidate_pairs)} candidatos en vez de ${integer(corpus.possible_pairs)} pares` },
    { técnica: `HNSW (${hnsw.config})`, exacta: 'fuerza bruta', pérdida: `recall@10 ${percent(hnsw.recall_at_10)} (IC ${percent(hnsw.recall_at_10_ci_low)}–${percent(hnsw.recall_at_10_ci_high)})`, ganancia: `${decimal(hnsw.speedup, 0)}× más rápida por consulta` },
    { técnica: `IVF (${ivf.config})`, exacta: 'fuerza bruta', pérdida: `recall@10 ${percent(ivf.recall_at_10)}`, ganancia: `${decimal(ivf.speedup, 0)}× más rápida; construcción ${decimal(ivf.build_seconds, 1)} s` },
    { técnica: `Bloom (${bytes(bloom.bytes)}, k = ${bloom.hashes})`, exacta: 'conjunto de pares', pérdida: `FPR final ${percent(bloom.final_fpr ?? bloom.observed_fpr, 2)} (en el flujo ${percent(bloom.observed_fpr, 2)}), sin falsos negativos`, ganancia: `${bytes(bloom.bytes)} frente a ${bytes(bloom.exact_bytes)}` },
    { técnica: `Count-Min (${cms.width} × ${cms.depth})`, exacta: 'diccionario de conteos', pérdida: `error relativo ${percent(cms.heavy_mean_relative_error)} en los 100 más frecuentes, ${percent(cms.tail_mean_relative_error)} en la cola`, ganancia: `${bytes(cms.bytes)} frente a ${bytes(cms.exact_bytes)}` },
    { técnica: `DGIM (${integer(dgim.window)} ${dgim.unit})`, exacta: 'ventana completa', pérdida: `error relativo final ${dgim.relative_error == null ? '—' : percent(dgim.relative_error)} (cota 50 %)`, ganancia: `${dgim.buckets} buckets frente a ${integer(dgim.window)} bits` },
  ]
  // Con caché, la etapa de procesamiento del manifiesto dura segundos: para proyectar se usa
  // la suma de sus subetapas medidas en la última ejecución real (metadata/processing_timings.json).
  const processingSeconds = Math.max(d.run.stages.find(s => s.name === 'processing')?.seconds || 0,
    d.processing_timings.reduce((t, s) => t + s.seconds, 0))
  const stages = d.run.stages.map(s => ({ etapa: s.name, motor: ENGINE[s.name] || '—', segundos: decimal(s.seconds, 1), participación: percent(s.seconds / d.run.stages.reduce((t, x) => t + x.seconds, 0)) }))
  const scaling = d.ann_scaling
  const fit = scaling.length > 1 ? linearFit(scaling.map(r => ({ x: r.corpus_size, y: r.exact_mean_ms }))) : null
  const n = scaling.at(-1)?.corpus_size || 0
  const projection = fit ? [1, 10, 100].map(f => ({
    escala: `${f}×`, procesos: integer(n * f),
    'búsqueda exacta / consulta': ms(fit.intercept + fit.slope * n * f),
    'matriz de vectores': bytes(scaling.at(-1).exact_matrix_mb * 1e6 * f),
    'pares posibles (LSH evita)': integer((n * f) * (n * f - 1) / 2),
    'procesamiento Spark (lineal)': `${decimal(processingSeconds * f / 60, 0)} min`,
  })) : []
  const spark = d.spark_vs_pandas.at(-1)
  const suppliers = d.quality.suppliers?.types || {}
  return <>
    <SectionHeader eyebrow="Parte VI · Discusión crítica" title="Qué aportó cada técnica y dónde termina la evidencia"
      description="Las cinco preguntas del enunciado, respondidas con las mediciones de este proyecto. La interpretación de cada una la redacta el equipo."
      question="¿Valieron la pena los métodos aproximados y Spark para responder Q1, Q2 y Q3?" />

    <FindingModule id="disc-valor" part="Parte VI · 1" question="¿Qué técnicas aportaron más valor concreto a las preguntas de interés y cuáles aportaron poco?">
      <DataTable rows={[
        { pregunta: 'Q1 Concentración', técnicas: 'Agregaciones Spark (HHI con ventanas), Bloom', evidencia: 'HHI por entidad y nivel; tasa de proveedores que repiten con la entidad' },
        { pregunta: 'Q2 Competencia', técnicas: 'Agregaciones Spark, FP-Growth dirigido, DGIM', evidencia: 'Tasa de postor único por dimensión; combinaciones con lift alto; conteo reciente' },
        { pregunta: 'Q3 Repetición', técnicas: 'Shingling, MinHash, LSH en Spark; HNSW/IVF', evidencia: `${integer(corpus.similar_pairs)} pares casi idénticos entre ${integer(corpus.documents)} procesos; buscador de vecinos` },
        { pregunta: 'Transversal', técnicas: 'Count-Min Sketch', evidencia: 'Frecuencias de proveedores y UNSPSC con memoria fija; con este volumen un diccionario exacto también cabe' },
      ]} />
    </FindingModule>

    <FindingModule id="disc-precision" part="Parte VI · 2" question="¿Cuánto se pierde en precisión con los métodos aproximados y vale la pena?">
      <DataTable rows={precision} />
    </FindingModule>

    <FindingModule id="disc-spark" part="Parte VI · 3" question="¿Qué tareas se beneficiaron realmente de Spark y cuáles eran manejables en local?">
      <div className="two-col">
        <Card title="Duración de cada etapa del pipeline" subtitle="Corrida completa sobre los datos reales"><DataTable rows={stages} /></Card>
        <Card title="Evidencia puntual" subtitle="Mediciones del propio proyecto">
          <div className="prose">
            <p><strong>Parseo de JSON anidado:</strong> {integer(d.quality.raw_records)} documentos en {decimal(d.processing_timings[0]?.seconds, 0)} s y el procesamiento completo (deduplicación, monedas y seis tablas Parquet) en {decimal(processingSeconds / 60, 1)} min, repartidos entre los núcleos del clúster. Con caché, las corridas siguientes lo reutilizan.</p>
            <p><strong>LSH sobre todo el corpus:</strong> firmas, buckets y verificación de {integer(corpus.candidate_pairs)} candidatos en {decimal(corpus.signatures_seconds + corpus.buckets_seconds + corpus.candidates_seconds + corpus.verification_seconds, 0)} s.</p>
            {spark && <p><strong>Una agregación simple</strong> sobre {integer(spark.processes)} procesos: Spark {decimal(spark.spark_seconds, 2)} s frente a pandas {decimal(spark.pandas_seconds, 2)} s ({decimal(spark.pandas_memory_mb, 0)} MB en memoria). A este volumen, pandas gana.</p>}
          </div>
        </Card>
      </div>
    </FindingModule>

    <FindingModule id="disc-escala" part="Parte VI · 4" question="¿Cómo cambiarían los resultados y las decisiones si el volumen fuera 10 o 100 veces mayor?">
      <Card title="Proyección a partir de lo medido" subtitle="Latencia exacta: ajuste lineal sobre el escalamiento de la Parte III; procesamiento: proporcional al volumen">
        <DataTable rows={projection} />
      </Card>
    </FindingModule>

    <FindingModule id="disc-etica" part="Parte VI · 5" question="¿Qué implicancias tiene aplicar estas técnicas sobre datos públicos peruanos?">
      <div className="risk-grid">
        <div><span>01</span><p><strong>Personas naturales.</strong> {integer(suppliers.persona_natural?.ids)} proveedores son personas naturales (RUC 10, que contiene el DNI). Un ranking por persona expone a individuos; aquí se publican agregados y los teléfonos de contacto del OCDS se descartan.</p></div>
        <div><span>02</span><p><strong>Consorcios opacos.</strong> Los consorcios reciben {percent(suppliers.consorcio?.amount_share)} del monto atribuido con identificadores de un solo uso: la concentración real por empresa no es observable con estos datos.</p></div>
        <div><span>03</span><p><strong>Señales, no acusaciones.</strong> Similitud, concentración o una regla con lift alto priorizan revisión; no prueban fraccionamiento, colusión ni irregularidad.</p></div>
        <div><span>04</span><p><strong>Calidad del registro.</strong> El número de postores falta en una parte de los procesos y el departamento es el domicilio del comprador: las conclusiones heredan esos vacíos.</p></div>
      </div>
    </FindingModule>
  </>
}
