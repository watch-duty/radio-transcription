import express from 'express';
import { getTranscript } from './transcripts/getTranscript.js';

const app = express();

app.get('/api/v1/transcript', getTranscript);

export const api = app;
