import express, { json, urlencoded } from 'express';

import cookieParser from 'cookie-parser';
import cors from 'cors';

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
app.use(cookieParser());

RegisterRoutes(app);

export const api = app;
