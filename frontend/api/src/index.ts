import express, { json, urlencoded } from 'express';

import cors from 'cors';
import { existsSync, readFileSync } from 'fs';
import { load } from 'js-yaml';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

import { ALLOWED_ORIGIN } from './config.js';
import { RegisterRoutes } from './generated/routes.js';

const app = express();

app.use(
  cors({
    origin: ALLOWED_ORIGIN,
    credentials: true,
  })
);

app.use(
  urlencoded({
    extended: true,
  })
);

app.use(json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function getOpenApiSpec() {
  const specPath = join(__dirname, '..', 'openapi.yaml');
  if (!existsSync(specPath)) {
    console.warn(`OpenAPI spec not found at ${specPath}`);
    return null;
  }

  const file = readFileSync(specPath, 'utf8');
  const swaggerDocument = load(file) as Record<string, unknown>;

  return swaggerDocument;
}

// Load the generated OpenAPI spec
try {
  const swaggerDocument = getOpenApiSpec();
  if (swaggerDocument) {
    app.get('/openapi.yaml', (req, res) => {
      // Dynamically set the server URL based on the request host to avoid hardcoding or environment variables
      const dynamicDoc = {
        ...swaggerDocument,
        servers: [{ url: `${req.protocol}://${req.get('host')}` }],
      };
      res.json(dynamicDoc);
    });
  }
} catch (error) {
  console.error('Failed to load OpenAPI spec:', error);
}

RegisterRoutes(app);

export const api = app;
