// Si la API falla, las páginas deben mostrar el error en lugar de quedarse cargando (B-01, B-02).
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import Concentration from '../pages/Concentration'
import Similarity from '../pages/Similarity'

let container

beforeEach(() => {
  container = document.createElement('div')
  document.body.appendChild(container)
  globalThis.fetch = vi.fn(async () => ({ ok: false, status: 503, json: async () => ({ detail: 'Artefacto no generado: x' }) }))
})

afterEach(() => { container.remove(); vi.restoreAllMocks() })

test.each([['Similarity', Similarity], ['Concentration', Concentration]])('%s muestra el error de la API', async (_, Page) => {
  const root = createRoot(container)
  await act(async () => { root.render(<Page navigate={() => {}} />) })
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)) })
  expect(container.textContent).toContain('No pudimos cargar esta sección')
  act(() => root.unmount())
})
