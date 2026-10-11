// El contenido escrito del reporte está completo: cada hallazgo tiene su interpretación,
// cada regla seleccionada la suya y cada caso de similitud sus campos. Si el pipeline
// cambia las reglas seleccionadas, este test avisa qué interpretación falta.
import fs from 'node:fs'
import path from 'node:path'
import { describe, expect, test } from 'vitest'
import cases from '../content/similarity_cases.json'
import rules from '../content/rules.json'

const CONTENT = path.resolve(__dirname, '../content/hallazgos')
const DATA = path.resolve(__dirname, '../../public/data')
const PAGES = path.resolve(__dirname, '../pages')

function findingIds() {
  // Todos los <FindingModule id="..."> de las páginas.
  const files = fs.readdirSync(PAGES, { recursive: true }).filter(f => String(f).endsWith('.jsx'))
  const ids = files.flatMap(f => [...fs.readFileSync(path.join(PAGES, String(f)), 'utf-8').matchAll(/FindingModule id="([^"]+)"/g)].map(m => m[1]))
  return [...new Set(ids)].sort()
}

describe('contenido del reporte', () => {
  test.each(findingIds())('el hallazgo %s tiene interpretación redactada', id => {
    const file = path.join(CONTENT, `${id}.md`)
    expect(fs.existsSync(file)).toBe(true)
    const text = fs.readFileSync(file, 'utf-8')
    expect(text).not.toContain('PENDIENTE')
    expect(text.trim().length).toBeGreaterThan(400)
  })

  test.skipIf(!fs.existsSync(path.join(DATA, 'association', '0.json')))('cada regla seleccionada tiene interpretación', () => {
    const selected = JSON.parse(fs.readFileSync(path.join(DATA, 'association', '0.json'), 'utf-8')).selected.rules
    expect(selected.length).toBeGreaterThanOrEqual(8)
    const missing = selected.map(r => r.rule_id).filter(id => !rules.reglas[id])
    expect(missing).toEqual([])
  })

  test('los casos de similitud están completos', () => {
    expect(cases.casos.length).toBeGreaterThanOrEqual(3)
    for (const c of cases.casos) {
      for (const field of ['left', 'right', 'titulo', 'tipo', 'interpretacion', 'limitacion', 'estado']) {
        expect(c[field], `${c.titulo}: ${field}`).toBeTruthy()
      }
    }
  })
})
