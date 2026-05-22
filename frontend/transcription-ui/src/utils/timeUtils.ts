import RelativeTimeFormat from 'relative-time-format';
import en from 'relative-time-format/locale/en';

export const MAX_WINDOW_DURATION_MS = 15 * 60 * 1000; // 15 minutes

RelativeTimeFormat.addLocale(en);
const rtf = new RelativeTimeFormat('en');

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

export function roundUpToNearestMinute(date: Date) {
  const msInMinute = 60 * 1000;
  return new Date(Math.ceil(date.getTime() / msInMinute) * msInMinute);
}

export function getRelativeTimeString(
  dateValue?: string | Date | number,
  capAtMinute = true
): string {
  if (!dateValue) return '';
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  const dateMs = date.getTime();
  if (Number.isNaN(dateMs)) {
    return '';
  }
  const now = new Date();
  const diffMs = now.getTime() - dateMs;
  const diffSeconds = Math.floor(diffMs / 1000);

  // Allows the option of showing second granularity.
  if (!capAtMinute) {
    if (diffSeconds < 10) {
      return '<10 seconds ago';
    }
    if (diffSeconds < 15) {
      return '<15 seconds ago';
    }
    if (diffSeconds < 30) {
      return '<30 seconds ago';
    }
  }

  if (diffSeconds < 60) {
    return '<1 minute ago';
  }

  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return rtf.format(-diffMinutes, 'minute');
  }

  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return rtf.format(-diffHours, 'hour');
  }

  const diffDays = Math.floor(diffHours / 24);
  return rtf.format(-diffDays, 'day');
}
