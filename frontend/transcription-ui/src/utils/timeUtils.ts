export const MAX_WINDOW_DURATION_MS = 30 * 60 * 1000; // 30 minutes

export function getInitialTimestamp(
  searchParams: URLSearchParams
): Date | null {
  const param = searchParams.get('timestamp');
  return param ? new Date(Number(param)) : null;
}

export function getSearchedStartTime(
  searchParams: URLSearchParams
): Date | null {
  const ts = searchParams.get('timestamp');
  if (ts) {
    return new Date(Number(ts) - 15 * 60 * 1000);
  }
  return null;
}

export function getSearchedEndTime(searchParams: URLSearchParams): Date | null {
  const ts = searchParams.get('timestamp');
  if (ts) {
    return new Date(Number(ts) + 15 * 60 * 1000);
  }
  return null;
}
