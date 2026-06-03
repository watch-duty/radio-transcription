import axios from 'axios';
import { GaxiosError } from 'gaxios';
import { GoogleAuth } from 'google-auth-library';

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

export async function getServiceClient(targetUrl: string) {
  const isProduction = process.env.NODE_ENV === 'production';
  if (process.env.BYPASS_AUTH === 'true' && !isProduction) {
    return {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      request: async <T>(config: any) => {
        return (await axios(config)) as { data: T };
      },
    };
  }

  // Production path: use Google Auth to get ID token client
  const auth = new GoogleAuth();
  return await auth.getIdTokenClient(targetUrl);
}
