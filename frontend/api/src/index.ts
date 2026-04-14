import express, { json, urlencoded } from 'express';

import cors from 'cors';
import swaggerUi from 'swagger-ui-express';
import * as fs from 'fs';
import * as yaml from 'js-yaml';
import * as path from 'path';

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

RegisterRoutes(app);

// Load the generated OpenAPI spec
try {
  const specPath = path.join(process.cwd(), 'openapi.yaml');
  if (fs.existsSync(specPath)) {
    const file = fs.readFileSync(specPath, 'utf8');
    const swaggerDocument = yaml.load(file) as any;
    app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));
  } else {
    console.warn(`OpenAPI spec not found at ${specPath}`);
  }
} catch (error) {
  console.error('Failed to load OpenAPI spec:', error);
}

export const api = app;
