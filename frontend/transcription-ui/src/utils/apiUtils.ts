import { ApiError } from '@transcription/common';

export async function apiFetch<T>(
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<T> {
  const response = await fetch(input, init);

  if (!response.ok) {
    let errorBody: unknown = null;
    let errorMessage: string | undefined;

    try {
      const text = await response.text();
      errorBody = text;
      errorMessage = `${response.status} ${response.statusText}`;
      if (text) {
        errorMessage += `: ${text}`;
      }

      const contentType = response.headers.get('content-type');

      if (contentType && contentType.includes('application/json')) {
        try {
          const json = JSON.parse(text);
          errorBody = json;

          if (json && typeof json === 'object') {
            const extractedMsg = json.message || json.detail;
            if (extractedMsg) {
              errorMessage = `${response.status} ${response.statusText}: ${extractedMsg}`;
            }
          }
        } catch {
          // Keep text as errorBody and errorMessage
        }
      }
    } catch {
      errorMessage = 'Failed to read error response body';
    }

    throw new ApiError(
      response.status,
      response.statusText,
      errorBody,
      errorMessage
    );
  }

  const text = await response.text();
  const contentType = response.headers.get('content-type');

  if (contentType && contentType.includes('application/json')) {
    return text ? JSON.parse(text) : ({} as T);
  }

  return text as unknown as T;
}
