import express from 'express';
import { listTranscripts } from './transcripts/listTranscripts.js';

const app = express();

app.get('/api/v1/transcripts', listTranscripts);

export const api = app;
