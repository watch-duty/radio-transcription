import eslintConfigPrettier from 'eslint-config-prettier/flat';
import react from 'eslint-plugin-react';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import { defineConfig, globalIgnores } from 'eslint/config';
import globals from 'globals';
import tseslint from 'typescript-eslint';

import css from '@eslint/css';
import pluginQuery from '@tanstack/eslint-plugin-query';

export default defineConfig([
  globalIgnores(['dist']),
  ...tseslint.configs.recommended,
  ...pluginQuery.configs['flat/recommended'],
  {
    files: ['**/*.{ts,tsx,js,jsx}'],
    ...reactHooks.configs.flat.recommended,
  },
  {
    files: ['**/*.{ts,tsx,js,jsx}'],
    ...reactRefresh.configs.vite,
  },
  {
    files: ['**/*.{ts,tsx}'],
    plugins: {
      react,
    },
    languageOptions: {
      parserOptions: {
        ecmaFeatures: {
          jsx: true,
        },
      },
      globals: {
        ...globals.browser,
      },
    },
  },
  {
    files: ['**/*.css'],
    plugins: { css },
    language: 'css/css',
  },
  eslintConfigPrettier,
]);
