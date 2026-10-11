// B-01 / B-02: si la API falla, las páginas deben mostrar el error en lugar de quedarse cargando.
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import Ann from '../pages/Ann'
import Similarity from '../pages/Similarity'

globalThis.IS_REACT_ACT_ENVIRONMENT = true
let container

beforeEach(() => {
  container = document.createElement('div')
  document.body.appendChild(container)
  globalThis.fetch = vi.fn(async () => ({ ok: false, status: 503, json: async () => ({ detail: 'Artefacto no generado: ann/hnsw_grid.csv' }) }))
})

afterEach(() => { container.remove(); vi.restoreAllMocks() })

async function render(Page) {
  const root = createRoot(container)
  await act(async () => { root.render(<Page />) })
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)) })
  return root
}

test.each([['Ann', Ann], ['Similarity', Similarity]])('%s muestra el error de la API', async (_, Page) => {
  const root = await render(Page)
  expect(container.textContent).toContain('No pudimos cargar esta sección')
  expect(container.textContent).not.toContain('Preparando resultados')
  act(() => root.unmount())
})
