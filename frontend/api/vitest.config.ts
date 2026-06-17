import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Excluding integration_tests from Vitest since that uses Playwright
    exclude: ['node_modules', 'dist', 'integration_tests'],
  },
});
