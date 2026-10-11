// Acceso a datos con dos modos:
// - local: la API FastAPI (/api/...), con todos los filtros.
// - estático (VITE_STATIC=1): JSON exportados por api/export_static.py, para publicar la web
//   sin servidor. Cada endpoint tiene un index.json que mapea la consulta canónica a su archivo.
export const STATIC = import.meta.env.VITE_STATIC === '1'
const BASE = import.meta.env.BASE_URL || '/'
const indexes = new Map()

function clean(params) {
  return Object.fromEntries(Object.entries(params).filter(([, value]) =>
    value !== undefined && value !== null && value !== '' && value !== 'Todos'))
}

export function canonical(params) {
  const entries = Object.entries(clean(params)).sort(([a], [b]) => a.localeCompare(b))
  return entries.map(([key, value]) => `${key}=${value}`).join('&')
}

async function readJson(url) {
  const response = await fetch(url)
  if (!response.ok) {
    let message = `Error ${response.status}`
    try { message = (await response.json()).detail || message } catch (_) { /* respuesta sin JSON */ }
    throw new Error(message)
  }
  return response.json()
}

async function staticApi(path, params) {
  const endpoint = path.replace(/^\/api\//, '')
  if (!indexes.has(endpoint)) indexes.set(endpoint, readJson(`${BASE}data/${endpoint}/index.json`))
  const index = await indexes.get(endpoint)
  // Solo cuentan los parámetros exportados; el resto (p. ej. categoría o mes en el EDA)
  // requiere la API local y se informa en la respuesta.
  const supported = Object.fromEntries(Object.entries(clean(params)).filter(([key]) => index.params.includes(key)))
  const ignored = Object.keys(clean(params)).filter(key => !index.params.includes(key))
  const file = index.files[canonical(supported)]
  if (!file) throw new Error('Combinación de filtros no disponible en la versión publicada; usar la versión local con API.')
  const data = await readJson(`${BASE}data/${endpoint}/${file}`)
  return ignored.length ? { ...data, static_ignored: ignored } : data
}

export async function api(path, params = {}) {
  if (STATIC) return staticApi(path, params)
  const search = new URLSearchParams(clean(params))
  return readJson(`${path}${search.size ? `?${search}` : ''}`)
}

export const money = (value) => new Intl.NumberFormat('es-PE', {
  style: 'currency', currency: 'PEN', notation: 'compact', maximumFractionDigits: 1,
}).format(value || 0)

export const integer = (value) => new Intl.NumberFormat('es-PE').format(Math.round(value || 0))
export const percent = (value, digits = 1) => `${((value || 0) * 100).toFixed(digits)}%`
export const decimal = (value, digits = 2) => Number(value || 0).toFixed(digits)
export const ms = (value) => `${Number(value || 0).toFixed(Number(value) < 1 ? 3 : 1)} ms`
export const bytes = (value) => {
  const v = Number(value || 0)
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)} MB`
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)} KB`
  return `${v} B`
}
