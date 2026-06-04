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
export async function getServiceClient(targetUrl: string) {
  const isProduction = process.env.NODE_ENV === 'production';
  if (AUTH_BACKEND === 'none' && !isProduction) {
    return {
      request: <T>(config: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
        return axios<T>(config);
      },
    };
  }

  // Production path: use Google Auth to get ID token client
  const auth = new GoogleAuth();
  return await auth.getIdTokenClient(targetUrl);
}
