import express, { json, urlencoded } from 'express';

import cors from 'cors';

import { ALLOWED_ORIGIN } from './config.js';
import { RegisterRoutes } from './generated/routes.js';

const app = express();

// Trust proxy headers (like X-Forwarded-Host) so Express can resolve the correct host
// when running behind the API Gateway. This allows DocsController to set the correct
// server URL in the spec for Swagger UI.
app.set('trust proxy', true);

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

export const api = app;
