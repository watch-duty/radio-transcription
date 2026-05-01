import { defineConfig } from 'vite';
import svgr from 'vite-plugin-svgr';

import react from '@vitejs/plugin-react-swc';

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    svgr({
      // svgr options: https://react-svgr.com/docs/options/
      svgrOptions: {
        jsxRuntime: 'automatic',
      },
      // A minimatch pattern, or array of patterns, which specifies the files in the build the plugin should include.
      include: '**/*.svg?react',
    }),
  ],
  optimizeDeps: {
    include: ['react/jsx-runtime'],
  },
  build: {
    outDir: 'dist',
  },
  server: {
    headers: { 'Cross-Origin-Opener-Policy': 'same-origin-allow-popups' },
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      '/openapi.yaml': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      // To prevent CORS errors with fetching from GCS on localhost
      '/gcs': {
        target: 'https://storage.googleapis.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/gcs/, ''),
      },
    },
  },
});
