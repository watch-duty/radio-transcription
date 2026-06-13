import { defineConfig, loadEnv, mergeConfig } from 'vite';
import type { UserConfig } from 'vite';

import react from '@vitejs/plugin-react-swc';

import { mockAuthPlugin } from './mockAuthPlugin';

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const apiTarget = env.VITE_PROXY_API_TARGET || 'http://localhost:8080';

  // Base configuration shared across all environments
  const baseConfig: UserConfig = {
    plugins: [react()],
    build: {
      outDir: 'dist',
    },
    server: {
      headers: { 'Cross-Origin-Opener-Policy': 'same-origin-allow-popups' },
    },
    optimizeDeps: {
      include: [
        'react',
        'react-dom',
        'react-router',
        '@mui/material',
        '@mui/icons-material',
        '@mui/material/styles',
        '@tanstack/react-query',
        'howler',
        'react-virtuoso',
        '@wavesurfer/react',
      ],
      exclude: ['@transcription/common'],
    },
  };

  if (env.VITE_AUTH_BACKEND === 'none') {
    baseConfig.plugins?.push(mockAuthPlugin);
  }

  // 1. Completely Local Dev Configuration (local-dev mode)
  const localConfig: UserConfig = {
    server: {
      proxy: {
        '/api': {
          target: apiTarget,
          changeOrigin: true,
        },
        '/openapi.yaml': {
          target: 'http://localhost:8080',
          changeOrigin: true,
        },
        '/gcs': {
          target: 'http://localhost:4443',
          changeOrigin: true,
          rewrite: (path) => {
            const match = path.match(/^\/gcs\/([^\/]+)\/(.+)$/);
            if (match) {
              const bucket = match[1];
              const object = match[2];
              return `/download/storage/v1/b/${bucket}/o/${encodeURIComponent(object)}?alt=media`;
            }
            return path.replace(/^\/gcs/, '');
          },
        },
      },
    },
  };

  // 2. Local-Remote Dev Configuration (dev mode proxying to remote secure APIs)
  const localRemoteConfig: UserConfig = {
    server: {
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
        '/gcs': {
          target: 'https://storage.googleapis.com',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/gcs/, ''),
        },
      },
    },
  };

  // 3. Production Configuration
  const prodConfig: UserConfig = {
    // Production has no proxies since it's served as static assets
  };

  const modeConfigs: Record<string, UserConfig> = {
    'local-dev': localConfig,
    dev: localRemoteConfig,
    production: prodConfig,
  };

  const activeConfig = modeConfigs[mode] || {};

  return mergeConfig(baseConfig, activeConfig);
});
