export function getInitialTimestamp(
  searchParams: URLSearchParams
): Date | null {
  const start = searchParams.get('startTimestamp');
  const end = searchParams.get('endTimestamp');
  if (start && end) {
    return new Date((Number(start) + Number(end)) / 2);
  }
  if (end) {
    return new Date(Number(end) - 60000);
  }
  return null;
}

export function getInitialDuration(searchParams: URLSearchParams): string {
  const start = searchParams.get('startTimestamp');
  const end = searchParams.get('endTimestamp');
  if (start && end) {
    const diffMs = Number(end) - Number(start);
    return String(Math.round(diffMs / (2 * 60000)));
  }
  return '';
}

export function getSearchedStartTime(
  searchParams: URLSearchParams
): Date | null {
  const start = searchParams.get('startTimestamp');
  return start ? new Date(Number(start)) : null;
}

export function getSearchedEndTime(searchParams: URLSearchParams): Date | null {
  const end = searchParams.get('endTimestamp');
  return end ? new Date(Number(end)) : null;
}

export function calculateSearchTimes(
  timestamp: Date | null,
  duration: string
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
      calcStart = null;
    }
  }
  return { startTime: calcStart, endTime: calcEnd };
}

export function validateDuration(duration: string): boolean {
  return !duration || (!isNaN(Number(duration)) && Number(duration) >= 0);
}
