import express, { json, urlencoded } from 'express';

import cors from 'cors';
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

// Load the generated OpenAPI spec
try {
  const specPath = path.join(process.cwd(), 'openapi.yaml');
  if (fs.existsSync(specPath)) {
    const file = fs.readFileSync(specPath, 'utf8');
    const swaggerDocument = yaml.load(file) as any;

    // Fix placeholders and security for local development
    if (swaggerDocument.servers) {
      swaggerDocument.servers = [{ url: 'http://localhost:8080' }];
    }

    if (swaggerDocument.components && swaggerDocument.components.securitySchemes) {
      // Change OAuth2 to HTTP Bearer for easier local testing
      swaggerDocument.components.securitySchemes.google_id_token = {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT'
      };
    }

    app.get('/openapi.json', (req, res) => {
      res.json(swaggerDocument);
    });
  } else {
    console.warn(`OpenAPI spec not found at ${specPath}`);
  }
} catch (error) {
  console.error('Failed to load OpenAPI spec:', error);
}

RegisterRoutes(app);

export const api = app;
