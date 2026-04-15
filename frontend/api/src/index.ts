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

RegisterRoutes(app);

export const api = app;
