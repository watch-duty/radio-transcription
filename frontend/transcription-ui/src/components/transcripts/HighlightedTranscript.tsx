import { Fragment, type ReactNode } from 'react';

import Box from '@mui/material/Box';
import type { RuleAnnotation } from '@transcription/common';

interface HighlightedTranscriptProps {
  text: string;
  ruleAnnotations: Record<string, RuleAnnotation> | undefined;
}

interface MatchInterval {
  start: number;
  end: number;
}

function collectIntervals(
  annotations: Record<string, RuleAnnotation> | undefined
): MatchInterval[] {
  if (!annotations) return [];
  const intervals: MatchInterval[] = [];
  for (const [ruleId, annotation] of Object.entries(annotations)) {
    if (!annotation.textMatch) continue;
    for (const span of annotation.textMatch) {
      // Zero-width spans (endIndex === startIndex) are legitimate — an
      // arbitrary regex rule can match empty — and are simply nothing to
      // highlight. An inverted span (endIndex < startIndex) violates a backend
      // invariant, so surface it rather than silently dropping it.
      if (span.endIndex < span.startIndex) {
        console.warn(
          `Ignoring rule annotation span with endIndex (${span.endIndex}) before startIndex (${span.startIndex}) for rule ${ruleId}. This indicates a backend bug.`
        );
        continue;
      }
      if (span.endIndex > span.startIndex) {
        intervals.push({ start: span.startIndex, end: span.endIndex });
      }
    }
  }
  intervals.sort(
    (a, b) => a.start - b.start || b.end - b.start - (a.end - a.start)
  );

  const merged: MatchInterval[] = [];
  for (const span of intervals) {
    const last = merged[merged.length - 1];
    if (last && span.start < last.end) {
      last.end = Math.max(last.end, span.end);
    } else {
      merged.push({ ...span });
    }
  }
  return merged;
}

export function HighlightedTranscript({
  text,
  ruleAnnotations,
}: HighlightedTranscriptProps) {
  const intervals = collectIntervals(ruleAnnotations);

  if (intervals.length === 0) {
    return <>{text}</>;
  }

  // Span offsets are codepoint indices from the backend; slice over a
  // codepoint array so non-BMP characters (emoji, etc.) don't shift matches.
  const chars = Array.from(text);
  const segments: ReactNode[] = [];
  let cursor = 0;
  intervals.forEach((interval) => {
    if (interval.start > cursor) {
      segments.push(
        <Fragment key={`text-${cursor}`}>
          {chars.slice(cursor, interval.start).join('')}
        </Fragment>
      );
    }
    segments.push(
      <Box
        key={`match-${interval.start}`}
        component="span"
        sx={{ color: 'warning.main', fontWeight: 600 }}
      >
        {chars.slice(interval.start, interval.end).join('')}
      </Box>
    );
    cursor = interval.end;
  });
  if (cursor < chars.length) {
    segments.push(
      <Fragment key={`text-${cursor}`}>{chars.slice(cursor).join('')}</Fragment>
    );
  }

  return <>{segments}</>;
}

export default HighlightedTranscript;
