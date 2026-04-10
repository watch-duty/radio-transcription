import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react-swc'

// https://vite.dev/config/
export default defineConfig(({ mode }: { mode: string }) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    plugins: [react()],
    build: {
      outDir: 'dist'
    },
    server: {
      headers: { 'Cross-Origin-Opener-Policy': 'same-origin-allow-popups' },
      proxy: {
        '/api': {
          target: env.VITE_API_PROXY_TARGET || 'http://localhost:8080',
          changeOrigin: true,
        },
      },
    }
  }
})
