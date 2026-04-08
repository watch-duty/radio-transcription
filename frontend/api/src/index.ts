import express, { json, urlencoded } from 'express';

import cors from 'cors';

import { WEB_URL } from './config.js';
import { RegisterRoutes } from './generated/routes.js';

const app = express();

app.use(
  cors({
    origin: WEB_URL,
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
