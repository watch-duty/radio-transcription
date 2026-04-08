export function isAxiosError(
  error: unknown
): error is { response?: { status: number } } {
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
