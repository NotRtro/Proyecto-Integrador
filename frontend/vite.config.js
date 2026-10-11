import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// VITE_BASE: ruta donde se publica (p. ej. /Proyecto-Integrador/ en GitHub Pages).
export default defineConfig({
  base: process.env.VITE_BASE || '/',
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8000' } },
  test: { environment: 'jsdom', setupFiles: ['src/__tests__/setup.js'] },
})
