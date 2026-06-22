import fs from 'fs';
import path from 'path';
import { defineConfig, loadEnv, mergeConfig } from 'vite';
import type { UserConfig } from 'vite';

import react from '@vitejs/plugin-react-swc';

import { mockAuthPlugin } from './mockAuthPlugin';

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const apiTarget = env.VITE_PROXY_API_TARGET || 'http://localhost:8080';

  const themeResolverPlugin = {
    name: 'theme-resolver',
    resolveId(source: string) {
      if (source.startsWith('@theme/')) {
        let file = source.replace('@theme/', '');
        const ext = path.extname(file);
        if (ext) {
          file = file.substring(0, file.length - ext.length);
        }
        const customThemePath = path.resolve(__dirname, 'src/theme/custom');
        const defaultThemePath = path.resolve(__dirname, 'src/theme/default');

        const candidatesMap: Record<string, string[]> = {
          palette: [
            'palette.ts',
            'palette.tsx',
            'palette/index.ts',
            'palette/index.tsx',
          ],
          breakpoints: [
            'breakpoints.ts',
            'breakpoints.tsx',
            'breakpoints/index.ts',
            'breakpoints/index.tsx',
          ],
          components: [
            'components.ts',
            'components.tsx',
            'components/index.ts',
            'components/index.tsx',
            'componentOverrides.ts',
            'componentOverrides.tsx',
            'componentOverrides/index.ts',
            'componentOverrides/index.tsx',
          ],
          typography: [
            'typography.ts',
            'typography.tsx',
            'typography/index.ts',
            'typography/index.tsx',
          ],
          favicon: ['favicon.svg', 'favicon.ico', 'favicon.png'],
        };

        const candidates = candidatesMap[file] || [`${file}.ts`, `${file}.tsx`];

        console.log(`[theme-resolver] Resolving source: "${source}"`);
        console.log(`[theme-resolver] env.VITE_THEME_DIR: "${env.VITE_THEME_DIR}"`);

        if (env.VITE_THEME_DIR) {
          for (const candidate of candidates) {
            const envFile = path.resolve(env.VITE_THEME_DIR, candidate);
            if (fs.existsSync(envFile)) {
              console.log(`[theme-resolver] Resolved to envFile: "${envFile}"`);
              return envFile;
            }
          }
        }

        for (const candidate of candidates) {
          const customFile = path.resolve(customThemePath, candidate);
          if (fs.existsSync(customFile)) {
            console.log(`[theme-resolver] Resolved to customFile: "${customFile}"`);
            return customFile;
          }
        }

        const defaultExt = file === 'favicon' ? 'svg' : 'ts';
        const defaultFile = path.resolve(defaultThemePath, `${file}.${defaultExt}`);
        console.log(`[theme-resolver] Default file: "${defaultFile}" (exists: ${fs.existsSync(defaultFile)})`);
        return defaultFile;
      }
      return null;
    },
  };

  // Base configuration shared across all environments
  const baseConfig: UserConfig = {
    plugins: [react(), themeResolverPlugin],
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
