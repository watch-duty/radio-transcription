// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { type HistogramMark } from '../../hooks/useTimelineHistogram';
import { TimelineMiniMap } from './TimelineMiniMap';
import { markColor } from './timelineMath';

const MINUTE = 60 * 1000;

const marks: HistogramMark[] = [
  { startMs: 0, endMs: 5 * MINUTE, count: 3, hasSpeech: true, hasAlert: false },
];

const renderMiniMap = (
  rangeStartMs: number,
  maxEnd: number,
  windowEndTime: number | null = null,
  onCenterWindow: (centerMs: number) => void = vi.fn()
) =>
  render(
    <TimelineMiniMap
      histogramMarks={marks}
      rangeStartMs={rangeStartMs}
      maxEnd={maxEnd}
      windowEndTime={windowEndTime}
      windowDurationMs={10 * MINUTE}
      isDarkTheme={false}
      onCenterWindow={onCenterWindow}
    />
  );

describe('TimelineMiniMap', () => {
  afterEach(cleanup);

  it('maps a click x-position to the center time of the range', () => {
    const onCenterWindow = vi.fn();
    renderMiniMap(0, 60 * MINUTE, null, onCenterWindow);
    const strip = screen.getByLabelText('timeline overview');
    // jsdom reports a zero-size rect, so stub a 100px-wide strip.
    vi.spyOn(strip, 'getBoundingClientRect').mockReturnValue({
      left: 0,
      width: 100,
    } as DOMRect);

    fireEvent.click(strip, { clientX: 25 });

    // 25% across a [0, 60m) range → 15m.
    expect(onCenterWindow).toHaveBeenCalledWith(15 * MINUTE);
  });

  it('does not move the window when the overview is hidden (range fits one window)', () => {
    const onCenterWindow = vi.fn();
    renderMiniMap(0, 5 * MINUTE, null, onCenterWindow);
    fireEvent.click(screen.getByLabelText('timeline overview'), {
      clientX: 25,
    });
    expect(onCenterWindow).not.toHaveBeenCalled();
  });

  it('shows the overview and viewport when the range exceeds one window', () => {
    renderMiniMap(0, 60 * MINUTE);
    expect(screen.getByLabelText('timeline overview')).toBeTruthy();
    expect(screen.getByTestId('minimap-viewport')).toBeTruthy();
  });

  it('hides the viewport when the range fits in one window', () => {
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

describe('markColor', () => {
  const mark = (hasSpeech: boolean, hasAlert: boolean): HistogramMark => ({
    startMs: 0,
    endMs: MINUTE,
    count: 1,
    hasSpeech,
    hasAlert,
  });

  it('paints alerts amber regardless of speech', () => {
    expect(markColor(mark(false, true), false)).toBe('warning.main');
    expect(markColor(mark(true, true), false)).toBe('warning.main');
  });

  it('paints speech (without alert) the same blue as the header/controls', () => {
    expect(markColor(mark(true, false), false)).toBe('primary.main');
  });

  it('paints non-speech a neutral grey per theme', () => {
    expect(markColor(mark(false, false), false)).toBe('grey.400');
    expect(markColor(mark(false, false), true)).toBe('grey.600');
  });
});
