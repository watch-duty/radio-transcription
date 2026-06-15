// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';

import { TimelineMiniMap } from './TimelineMiniMap';
import { type HistogramMark } from './timelineMath';

const MINUTE = 60 * 1000;

const marks: HistogramMark[] = [
  { startMs: 0, endMs: 5 * MINUTE, count: 3, hasAlert: false },
];

const renderMiniMap = (
  rangeStartMs: number,
  maxEnd: number,
  windowEndTime: number | null = null
) =>
  render(
    <TimelineMiniMap
      histogramMarks={marks}
      rangeStartMs={rangeStartMs}
      maxEnd={maxEnd}
      windowEndTime={windowEndTime}
      windowDurationMs={10 * MINUTE}
      isDarkTheme={false}
      onScrubToCenter={vi.fn()}
    />
  );

describe('TimelineMiniMap', () => {
  afterEach(cleanup);

  it('shows the overview when the loaded range exceeds one window', () => {
    renderMiniMap(0, 60 * MINUTE);
    expect(screen.getByLabelText('timeline overview')).toBeTruthy();
    expect(screen.getByTestId('minimap-viewport')).toBeTruthy();
  });

  it('hides the overview when the range fits in one window', () => {
    renderMiniMap(0, 5 * MINUTE);
    expect(screen.queryByTestId('minimap-viewport')).toBeNull();
  });

  it('hides the viewport cursor when the window is before the range', () => {
    // Range is [0, 60m); a window ending before it (older date filter) → no cursor.
    renderMiniMap(0, 60 * MINUTE, -30 * MINUTE);
    expect(screen.getByLabelText('timeline overview')).toBeTruthy();
    expect(screen.queryByTestId('minimap-viewport')).toBeNull();
  });
});
