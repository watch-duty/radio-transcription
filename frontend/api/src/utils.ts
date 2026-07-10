import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';
import { GaxiosError } from 'gaxios';
import { GoogleAuth } from 'google-auth-library';

import { AUTH_BACKEND } from './config.js';

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message);
  }
}

export type BackendErrorResponse = {
  status: number;
  message: string;
};

export function handleBackendError(
  error: unknown,
  defaultMessage: string
): BackendErrorResponse {
  if (error instanceof GaxiosError) {
    const status = error.response?.status || 500;
    const message =
      error.response?.data?.detail || error.message || defaultMessage;
    console.error(
      JSON.stringify({
        level: 'ERROR',
        status,
        message,
        data: error.response?.data,
      })
    );
    return {
      status,
      message: message || defaultMessage,
    };
  }

  const status = 500;
  const message = error instanceof Error ? error.message : String(error);
  console.error(
    JSON.stringify({
      level: 'ERROR',
      status,
      message: `Unexpected error ${message}`,
    })
  );
  return {
    status,
    message: message || defaultMessage,
  };
}

interface ServiceResponse<T> {
  data: T;
  status?: number;
  statusText?: string;
  headers?: unknown;
  config?: unknown;
}

interface ServiceClient {
  request<T = unknown>(config: AxiosRequestConfig): Promise<ServiceResponse<T>>;
}

const clientFactories: Record<
  string,
  (targetUrl: string) => Promise<ServiceClient>
> = {
  none: async () => ({
    request: <T>(config: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
      return axios<T>(config);
    },
  }),
  google: async (targetUrl: string) => {
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(targetUrl);
    return client as unknown as ServiceClient;
  },
};

/**
 * Returns an HTTP client to communicate with downstream services.
 *
 * If running in a non-production environment with `AUTH_BACKEND` set to `'none'`
 * (typically during local development or in test suites where authentication is bypassed),
 * it returns a simplified client wrapper using unauthenticated `axios`.
 *
 * Otherwise, it uses `google-auth-library` to construct and return an authenticated
 * `IdTokenClient` using Google application default credentials to call the target service.
 *
 * @param targetUrl The base URL of the service we want to request.
 * @returns An authenticated client or an unauthenticated axios fallback wrapper.
 */
export async function getServiceClient(
  targetUrl: string
): Promise<ServiceClient> {
  const isProduction = process.env.NODE_ENV === 'production';
  const backend = !isProduction && AUTH_BACKEND === 'none' ? 'none' : 'google';

  const factory = clientFactories[backend];
  if (!factory) {
    throw new Error(`Unsupported AUTH_BACKEND: ${backend}`);
  }
  return factory(targetUrl);
}

/**
 * Converts an object's keys from snake_case to camelCase recursively.
 */
export function toCamel<T = unknown>(obj: unknown): T {
  if (obj === null || typeof obj !== 'object') {
    return obj as T;
  }
  if (Array.isArray(obj)) {
    return obj.map((item) => toCamel(item)) as unknown as T;
  }
  return Object.entries(obj as Record<string, unknown>).reduce(
    (acc, [key, value]) => {
      const camelKey = key.replace(/_([a-z])/g, (_, letter) =>
        letter.toUpperCase()
      );
      acc[camelKey] = toCamel(value);
      return acc;
    },
    {} as Record<string, unknown>
  ) as T;
}
