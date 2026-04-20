export const DEFAULT_DURATION_MINUTES = 5;

export function getInitialTimestamp(
  searchParams: URLSearchParams
): Date | null {
  const param = searchParams.get('timestamp');
  return param ? new Date(Number(param)) : null;
}

export function getInitialDuration(
  searchParams: URLSearchParams
): string | null {
  return searchParams.get('duration');
}

export function getSearchedStartTime(
  searchParams: URLSearchParams
): Date | null {
  const ts = searchParams.get('timestamp');
  const dur = searchParams.get('duration');
  if (ts && dur && dur.trim() !== '') {
    return new Date(Number(ts) - Number(dur) * 60000);
  }
  return null;
}

export function getSearchedEndTime(searchParams: URLSearchParams): Date | null {
  const ts = searchParams.get('timestamp');
  const dur = searchParams.get('duration');
  if (ts) {
    if (dur && dur.trim() !== '') {
      return new Date(Number(ts) + Number(dur) * 60000 + 60000);
    }
    return new Date(Number(ts) + 60000);
  }
  return null;
}

export function calculateSearchTimes(
  timestamp: Date | null,
  duration: string | null
): { startTime: Date | null; endTime: Date | null } {
  let calcStart: Date | null = null;
  let calcEnd: Date | null = null;
  if (timestamp) {
    if (duration && duration.trim() !== '') {
      const mins = Number(duration);
      const offsetMs = mins * 60000;
      calcStart = new Date(timestamp.getTime() - offsetMs);
      calcEnd = new Date(timestamp.getTime() + offsetMs + 60000);
    } else {
      calcEnd = new Date(timestamp.getTime() + 60000);
    }
  }
  return { startTime: calcStart, endTime: calcEnd };
}

export function validateDuration(duration: string): boolean {
  return !duration || (!isNaN(Number(duration)) && Number(duration) >= 0);
}
