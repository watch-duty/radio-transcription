import { GaxiosError } from 'gaxios';

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
