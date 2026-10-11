// Cada página se monta con los datos reales exportados por api/export_static.py
// (frontend/public/data). Si no se han exportado, el test se omite.
import fs from 'node:fs'
import path from 'node:path'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeAll, describe, expect, test, vi } from 'vitest'

const DATA = path.resolve(__dirname, '../../public/data')
const hasData = fs.existsSync(path.join(DATA, 'meta', 'index.json'))
const PAGES = ['Overview', 'Data', 'Eda', 'Concentration', 'Competition', 'Similarity', 'Signals', 'Lab', 'Discussion',
  'lab/AnnLab', 'lab/StreamLab', 'lab/RulesLab']

describe.skipIf(!hasData)('páginas con datos exportados', () => {
  let pages
  beforeAll(async () => {
    vi.stubEnv('VITE_STATIC', '1')
    globalThis.fetch = vi.fn(async url => {
      const file = path.join(DATA, decodeURIComponent(String(url)).replace(/^.*\/data\//, ''))
      if (!fs.existsSync(file)) return { ok: false, status: 404, json: async () => ({ detail: `falta ${file}` }) }
      return { ok: true, status: 200, json: async () => JSON.parse(fs.readFileSync(file, 'utf-8')) }
    })
    const modules = import.meta.glob('../pages/**/*.jsx')
    pages = Object.fromEntries(await Promise.all(PAGES.map(async name => [name, (await modules[`../pages/${name}.jsx`]()).default])))
  }, 120000)
  afterEach(() => { document.body.innerHTML = '' })

  test.each(PAGES)('%s se renderiza sin errores', async name => {
    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    const Page = pages[name]
    await act(async () => { root.render(<Page navigate={() => {}} />) })
    for (let i = 0; i < 5; i++) await act(async () => { await new Promise(resolve => setTimeout(resolve, 20)) })
    expect(container.textContent).not.toContain('No pudimos cargar esta sección')
    // Las páginas tienen título; los módulos del laboratorio, al menos un hallazgo.
    expect(container.querySelector('h1, .finding')).not.toBeNull()
    act(() => root.unmount())
  }, 60000)
})
