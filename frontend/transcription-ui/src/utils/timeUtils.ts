import RelativeTimeFormat from 'relative-time-format';
import en from 'relative-time-format/locale/en';

export const AUDIO_WINDOW_DURATION_MS = 15 * 60 * 1000; // 15 minutes

RelativeTimeFormat.addLocale(en);
const rtf = new RelativeTimeFormat('en');

// 24-hour HH:MM, used by the audio timeline labels and the date/time chip so
// they stay consistent regardless of locale AM/PM conventions. Single choke
// point for clock rendering — route a future user 12h/24h setting through here.
export function formatClockTime(timestamp: number): string {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
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

export function formatDuration(seconds: number): string {
  const roundedSeconds = Math.round(seconds);
  if (roundedSeconds < 60) {
    return `${roundedSeconds} sec`;
  }
  const minutes = Math.floor(roundedSeconds / 60);
  const remainingSeconds = roundedSeconds % 60;
  if (remainingSeconds === 0) {
    return `${minutes} min`;
  }
  return `${minutes} min ${remainingSeconds} sec`;
}
