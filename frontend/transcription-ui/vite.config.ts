import { defineConfig } from 'vite';

import react from '@vitejs/plugin-react-swc';

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const isLocalDev = mode === 'local-dev';

  return {
    plugins: [react()],
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
          target: isLocalDev
            ? 'http://localhost:4443'
            : 'https://storage.googleapis.com',
          changeOrigin: true,
          rewrite: (path) => {
            if (isLocalDev) {
              const match = path.match(/^\/gcs\/([^\/]+)\/(.+)$/);
              if (match) {
                const bucket = match[1];
                const object = match[2];
                return `/download/storage/v1/b/${bucket}/o/${encodeURIComponent(object)}?alt=media`;
              }
            }
            return path.replace(/^\/gcs/, '');
          },
        },
      },
    },
  };
});
