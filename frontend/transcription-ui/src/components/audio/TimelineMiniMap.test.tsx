// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';

import { TimelineMiniMap } from './TimelineMiniMap';
import { type TranscriptTime } from './timelineMath';

const MINUTE = 60 * 1000;

const times: TranscriptTime[] = [
  { id: 'a', startMs: 0, endMs: 5000, hasAlert: false },
];

const renderMiniMap = (rangeStartMs: number, maxEnd: number) =>
  render(
    <TimelineMiniMap
      transcriptTimes={times}
      rangeStartMs={rangeStartMs}
      maxEnd={maxEnd}
      windowEndTime={null}
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
});
