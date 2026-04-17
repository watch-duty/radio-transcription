import express from 'express';

import { existsSync, readFileSync } from 'fs';
import { load } from 'js-yaml';
import { dirname, join } from 'path';
import {
  Controller,
  Extension,
  Get,
  Request,
  Route,
  Security,
  Tags,
} from 'tsoa';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export interface OpenApiSpec {
  [key: string]: unknown;
}

@Route('api/v1/docs')
@Tags('Docs')
export class DocsController extends Controller {
  @Get('openapi.json')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getSpec(
    @Request() request: express.Request
  ): Promise<OpenApiSpec> {
    // __dirname is frontend/api/src/docs
    // .. goes to src
    // .. goes to api root
    const specPath = join(__dirname, '..', '..', 'openapi.yaml');

    if (!existsSync(specPath)) {
      this.setStatus(404);
      throw new Error(`OpenAPI spec not found at ${specPath}`);
    }

    const file = readFileSync(specPath, 'utf8');
    const spec = load(file) as Record<string, unknown>;

    const protocol = request.get('host')?.includes('localhost') ? 'http' : 'https';
    spec.servers = [
      {
        url: `${protocol}://${request.get('host')}`,
      },
    ];

    return spec;
  }
}
