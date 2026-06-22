import { defineConfig, mergeConfig } from 'vitest/config';
import viteConfig from './vite.config';

export default defineConfig((configEnv) => {
  const baseViteConfig = typeof viteConfig === 'function' ? viteConfig(configEnv) : viteConfig;
  return mergeConfig(
    baseViteConfig,
    {
      test: {
        setupFiles: ['./src/test/setup.ts'],
      },
    }
  );
});
