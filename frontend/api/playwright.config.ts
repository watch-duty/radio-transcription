/// <reference types="node" />
import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '../../integration_tests/frontend',
  timeout: 30000,
  expect: {
    timeout: 5000,
  },
  reporter: 'list',
  use: {
    baseURL:
      process.env.VITE_API_BASE_URL ||
      `http://localhost:${process.env.PORT || 8080}`,
    extraHTTPHeaders: {
      Accept: 'application/json',
    },
  },
  webServer: process.env.VITE_API_BASE_URL
    ? undefined
    : {
        command: 'yarn local',
        url: 'http://localhost:8080/api/v1/feeds',
        reuseExistingServer: true,
        timeout: 120000,
      },
});
