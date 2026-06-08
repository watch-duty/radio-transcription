import { Fragment, type ReactNode } from 'react';

import Box from '@mui/material/Box';
import type { RuleAnnotation } from '@transcription/common';

interface HighlightedTranscriptProps {
  text: string;
  ruleAnnotations: RuleAnnotation[] | undefined;
}

interface MatchInterval {
  start: number;
  end: number;
}

function collectIntervals(
  annotations: RuleAnnotation[] | undefined
): MatchInterval[] {
  if (!annotations) return [];
  const intervals: MatchInterval[] = [];
  for (const annotation of annotations) {
    if (!annotation.textMatch) continue;
    for (const span of annotation.textMatch.spans) {
      if (span.end > span.start) {
        intervals.push({ start: span.start, end: span.end });
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
  intervals.forEach((interval, i) => {
    if (interval.start > cursor) {
      segments.push(
        <Fragment key={`text-${cursor}`}>
          {chars.slice(cursor, interval.start).join('')}
        </Fragment>
      );
    }
    segments.push(
      <Box
        key={`match-${interval.start}-${i}`}
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
