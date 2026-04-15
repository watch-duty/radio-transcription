import express, { json, urlencoded } from 'express';

import cors from 'cors';
import * as fs from 'fs';
import * as yaml from 'js-yaml';
import * as path from 'path';
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
const __dirname = path.dirname(__filename);

function getOpenApiSpec() {
  const specPath = path.join(__dirname, '..', 'openapi.yaml');
  if (!fs.existsSync(specPath)) {
    console.warn(`OpenAPI spec not found at ${specPath}`);
    return null;
  }
  
  const file = fs.readFileSync(specPath, 'utf8');
  const swaggerDocument = yaml.load(file) as any;

  // Fix placeholders and security for local development
  if (swaggerDocument.servers) {
    if (!process.env.SWAGGER_SERVER_URL) {
      throw new Error('SWAGGER_SERVER_URL environment variable is required but not set.');
    }
    swaggerDocument.servers = [{ url: process.env.SWAGGER_SERVER_URL }];
  }

  if (swaggerDocument.components && swaggerDocument.components.securitySchemes) {
    // Change OAuth2 to HTTP Bearer for easier local testing
    swaggerDocument.components.securitySchemes.google_id_token = {
      type: 'http',
      scheme: 'bearer',
      bearerFormat: 'JWT'
    };
  }

  return swaggerDocument;
}

// Load the generated OpenAPI spec
try {
  const swaggerDocument = getOpenApiSpec();
  if (swaggerDocument) {
    app.get('/openapi.json', (req, res) => {
      res.json(swaggerDocument);
    });
  }
} catch (error) {
  console.error('Failed to load OpenAPI spec:', error);
}

RegisterRoutes(app);

export const api = app;
