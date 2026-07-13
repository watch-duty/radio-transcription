import RelativeTimeFormat from 'relative-time-format';
import en from 'relative-time-format/locale/en';

import { type AudioSegment } from '@transcription/common';

export const MAX_WINDOW_DURATION_MS = 15 * 60 * 1000; // 15 minutes

// Span of the 24h timeline overview (mini-map), and the amount the list eagerly
// preloads to back it.
export const TIMELINE_RANGE_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

RelativeTimeFormat.addLocale(en);
const rtf = new RelativeTimeFormat('en');

// Newest loaded segment's end. Scan rather than read segments[0] — the list is
// sorted by start time, not end. Null when there are no segments.
export function getLiveEdgeMs(segments: AudioSegment[]): number | null {
  let liveEdgeMs = -Infinity;
  for (const segment of segments) {
    const end = new Date(segment.endTimestamp).getTime();
    if (end > liveEdgeMs) liveEdgeMs = end;
  }
  return liveEdgeMs === -Infinity ? null : liveEdgeMs;
}

// 24-hour HH:MM (optionally :SS), locale-independent, for the timeline playhead.
export function formatClockTime(
  timestamp: number,
  includeSeconds = false
): string {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    ...(includeSeconds && { second: '2-digit' }),
    hour12: false,
  });
}

// Short month/day, e.g. "Jun 26" — the timeline overview's day-boundary labels.
export function formatMonthDay(timestamp: number): string {
  return new Date(timestamp).toLocaleDateString([], {
    month: 'short',
    day: 'numeric',
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

export function formatDuration(
  seconds: number,
  showSeconds: boolean = true
): string {
  if (seconds < 1) {
    return showSeconds ? '<1 sec' : '<1 min';
  }
  const roundedSeconds = Math.round(seconds);
  if (roundedSeconds < 60) {
    return showSeconds ? `${roundedSeconds} sec` : '<1 min';
  }
  const minutes = Math.floor(roundedSeconds / 60);
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;

  if (!showSeconds) {
    if (hours === 0) {
      return `${minutes} min`;
    }
    if (remainingMinutes === 0) {
      return `${hours} hr`;
    }
    return `${hours} hr ${remainingMinutes} min`;
  }

  const remainingSeconds = roundedSeconds % 60;
  if (hours === 0) {
    if (remainingSeconds === 0) {
      return `${minutes} min`;
    }
    return `${minutes} min ${remainingSeconds} sec`;
  }

  const parts = [`${hours} hr`];
  if (remainingMinutes > 0) {
    parts.push(`${remainingMinutes} min`);
  }
  if (remainingSeconds > 0) {
    parts.push(`${remainingSeconds} sec`);
  }
  return parts.join(' ');
}
