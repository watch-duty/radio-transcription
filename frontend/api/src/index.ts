import express, { json, urlencoded } from 'express';

import cookieParser from 'cookie-parser';
import cors from 'cors';

import { ALLOWED_ORIGIN } from './config.js';
import { RegisterRoutes } from './generated/routes.js';
import { HttpError } from './utils.js';

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

// Centralized error handling for all backend route errors. Must be registered after all routes.
app.use(
  (
    err: Error,
    req: express.Request,
    res: express.Response,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    next: express.NextFunction
  ) => {
    console.error(err);

    if (err instanceof HttpError) {
      res.status(err.status).json({ message: err.message });
      return;
    }

    res.status(500).json({ message: 'Internal Server Error' });
  }
);

export const api = app;
