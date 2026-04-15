import express from 'express';

import { existsSync, readFileSync } from 'fs';
import { load } from 'js-yaml';
import { dirname, join } from 'path';
import { Controller, Get, Hidden, Request, Route, Security, Tags } from 'tsoa';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

@Route('api/v1/docs')
@Tags('Docs')
export class DocsController extends Controller {
  @Hidden()
  @Get('openapi.json')
  @Security('google_id_token')
  public async getSpec(
    @Request() request: express.Request
  ): Promise<Record<string, unknown>> {
    // __dirname is frontend/api/src/docs
    // .. goes to src
    // .. goes to api root
    const specPath = join(__dirname, '..', '..', 'openapi.yaml');

    if (!existsSync(specPath)) {
      this.setStatus(404);
      throw new Error(`OpenAPI spec not found at ${specPath}`);
    }

    const file = readFileSync(specPath, 'utf8');
    const swaggerDocument = load(file) as Record<string, unknown>;

    // Dynamically set the server URL based on the request host
    const dynamicDoc = {
      ...swaggerDocument,
      servers: [{ url: `${request.protocol}://${request.get('host')}` }],
    };

    return dynamicDoc;
  }
}
