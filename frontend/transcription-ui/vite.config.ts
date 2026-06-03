import { defineConfig, loadEnv } from 'vite';

import react from '@vitejs/plugin-react-swc';

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const isLocalDev = mode === 'local-dev';
  const env = loadEnv(mode, process.cwd(), '');
  const apiTarget = env.VITE_PROXY_API_TARGET || 'http://localhost:8080';

  return {
    plugins: [
      react(),
      ...(env.VITE_AUTH_BACKEND === 'none'
        ? [
            {
              name: 'mock-auth',
              configureServer(server) {
                server.middlewares.use((req, res, next) => {
                  if (
                    req.url === '/api/v1/auth/google' ||
                    req.url === '/api/v1/auth/session'
                  ) {
                    const header = Buffer.from(
                      JSON.stringify({ alg: 'none', typ: 'JWT' })
                    ).toString('base64');
                    const payload = Buffer.from(
                      JSON.stringify({
                        email: 'local-dev@example.com',
                        email_verified: true,
                        sub: 'local-dev',
                        aud: 'local-dev-aud',
                        iss: 'https://accounts.google.com',
                        exp: Math.floor(Date.now() / 1000) + 24 * 60 * 60,
                      })
                    ).toString('base64');
                    res.setHeader('Content-Type', 'application/json');
                    res.end(
                      JSON.stringify({
                        idToken: `${header}.${payload}.mocksignature`,
                      })
                    );
                  } else {
                    next();
                  }
                });
              },
            },
          ]
        : []),
    ],
    build: {
      outDir: 'dist',
    },
    server: {
      headers: { 'Cross-Origin-Opener-Policy': 'same-origin-allow-popups' },
      proxy: {
        '/api': {
          target: apiTarget,
          changeOrigin: true,
          secure: apiTarget.startsWith('https'),
          configure: (proxy) => {
            proxy.on('proxyReq', (proxyReq) => {
              if (apiTarget.startsWith('https') && env.VITE_PROXY_API_ORIGIN) {
                proxyReq.setHeader('origin', env.VITE_PROXY_API_ORIGIN);
              }
            });
          },
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
