export function isAxiosError(
  error: unknown
): error is { response?: { status: number; data?: unknown } } {
  if (typeof error !== 'object' || error === null) {
    return false;
  }
  const err = error as Record<string, unknown>;
  if (!('response' in err)) {
    return false;
  }
  const response = err.response as Record<string, unknown>;
  return (
    typeof response === 'object' && response !== null && 'status' in response
  );
}

export function handleBackendError(
  error: unknown,
  actionMessage: string
): never {
  if (isAxiosError(error)) {
    const status = error.response?.status || 500;
    const data = JSON.stringify(error.response?.data);
    console.error(
      JSON.stringify({
        level: 'ERROR',
        message: `Backend API error: ${status}`,
        data: error.response?.data,
      })
    );
    throw new Error(`Backend API error ${status}: ${data}`, { cause: error });
  }
  console.error(
    JSON.stringify({
      level: 'ERROR',
      message: `Unexpected error ${actionMessage}`,
      error: error instanceof Error ? error.message : String(error),
    })
  );
  throw new Error(`Error ${actionMessage}`, { cause: error });
}
