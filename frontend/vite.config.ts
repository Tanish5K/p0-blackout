import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The Vite dev server proxies /ws to the Go backend so the browser only talks
// to :5173 (no CORS) while the backend still performs its own Origin check.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true,
        changeOrigin: false,
      },
      '/healthz': {
        target: 'http://localhost:8080',
        changeOrigin: false,
      },
    },
  },
})
