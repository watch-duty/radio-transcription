import axios from 'axios';
import { GoogleAuth } from 'google-auth-library';
import { load } from 'js-yaml';
import { Controller, Extension, Get, Route, Security, Tags } from 'tsoa';

import { API_PUBLIC_URL, PROJECT_ID } from '../config.js';

export interface OpenApiSpec {
  [key: string]: unknown;
}

let cachedSpec: OpenApiSpec | null = null;

@Route('api/v1/docs')
@Tags('Docs')
export class DocsController extends Controller {
  private async getOpenApiSpec(): Promise<OpenApiSpec | null> {
    const auth = new GoogleAuth();
    const client = await auth.getClient();
    const tokenResponse = await client.getAccessToken();
    const token = tokenResponse.token;

    if (!token) {
      console.error('Failed to get access token');
      return null;
    }

    // TODO: Currently assuming we only have one API config.
    // To make this more robust, we should update the API ID to
    // be an environment variable.
    const apisUrl = `https://apigateway.googleapis.com/v1/projects/${PROJECT_ID}/locations/global/apis`;
    const apisResponse = await axios.get(apisUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const apis = apisResponse.data.apis;
    if (!apis || apis.length === 0) {
      console.error(`No APIs found in project ${PROJECT_ID}`);
      return null;
    }
    const apiName = apis[0].name;

    // List configs for the API to get the name
    const listUrl = `https://apigateway.googleapis.com/v1/${apiName}/configs`;
    const listResponse = await axios.get(listUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const configs = listResponse.data.apiConfigs;
    if (!configs || configs.length === 0) {
      console.error(`No configs found for API ${apiName}`);
      return null;
    }
    // Sort by createTime descending
    configs.sort(
      (a: { createTime: string }, b: { createTime: string }) =>
        new Date(b.createTime).getTime() - new Date(a.createTime).getTime()
    );
    const latestConfig = configs[0];
    const configName = latestConfig.name;

    // Finally get the open api spec from the config
    const getUrl = `https://apigateway.googleapis.com/v1/${configName}?view=FULL`;
    const getResponse = await axios.get(getUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const config = getResponse.data;
    const openapiDocs = config.openapiDocuments;

    if (!openapiDocs || !openapiDocs.length) {
      console.error(`No OpenAPI documents found in config ${configName}`);
      return null;
    }

    const contents = openapiDocs[0].document.contents;
    const decoded = Buffer.from(contents, 'base64').toString('utf8');
    return load(decoded) as OpenApiSpec;
  }

  @Get('openapi.json')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getSpec(): Promise<OpenApiSpec | null> {
    if (!cachedSpec) {
      // Get config from API Gateway
      cachedSpec = await this.getOpenApiSpec();
    }

    if (!cachedSpec) {
      this.setStatus(500);
      return null;
    }

    if (API_PUBLIC_URL) {
      cachedSpec.servers = [
        {
          url: API_PUBLIC_URL,
        },
      ];
    }
    return cachedSpec;
  }
}
