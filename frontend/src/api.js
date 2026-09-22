export async function api(path, params = {}) {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '' && value !== 'Todos') search.set(key, value)
  })
  const response = await fetch(`${path}${search.size ? `?${search}` : ''}`)
  if (!response.ok) {
    let message = `Error ${response.status}`
    try { message = (await response.json()).detail || message } catch (_) { /* response without JSON */ }
    throw new Error(message)
  }
  return response.json()
}

export const money = (value) => new Intl.NumberFormat('es-PE', {
  style: 'currency', currency: 'PEN', notation: 'compact', maximumFractionDigits: 1,
}).format(value || 0)

export const integer = (value) => new Intl.NumberFormat('es-PE').format(Math.round(value || 0))
export const percent = (value, digits = 1) => `${((value || 0) * 100).toFixed(digits)}%`
export const ms = (value) => `${Number(value || 0).toFixed(2)} ms`
