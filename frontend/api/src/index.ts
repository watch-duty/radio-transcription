import express, { json, urlencoded } from 'express';

import { RegisterRoutes } from './generated/routes.js';

const app = express();

app.use(
  urlencoded({
    extended: true,
  })
);

app.use(json());

RegisterRoutes(app);

export const api = app;
