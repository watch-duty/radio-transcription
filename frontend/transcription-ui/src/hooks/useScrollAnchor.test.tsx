// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { renderHook } from '@testing-library/react';

import { type RenderableAudioSegment } from './useConsolidatedAudioSegments';
import { VIRTUOSO_START_INDEX, useScrollAnchor } from './useScrollAnchor';

const seg = (id: string): RenderableAudioSegment =>
  ({ id }) as RenderableAudioSegment;

const bundle = (
  id: string,
  bundledSegmentIds: string[]
): RenderableAudioSegment =>
  ({
    id,
    isNonSpeechBundle: true,
    bundledSegmentIds,
  }) as RenderableAudioSegment;

type Props = Parameters<typeof useScrollAnchor>[0];

function setup(initialProps: Props) {
  return renderHook((props: Props) => useScrollAnchor(props), {
    initialProps,
  });
}

describe('useScrollAnchor', () => {
  it('starts at the base index and does not shift on the first render', () => {
    const { result } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });
    expect(result.current).toBe(VIRTUOSO_START_INDEX);
  });

  it('lowers the index by the number of items prepended above the prior head', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    // Two newer segments prepended; the prior head 'a' is now at index 2.
    rerender({
      headId: 'n2',
      renderedSegments: [seg('n2'), seg('n1'), seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    expect(result.current).toBe(VIRTUOSO_START_INDEX - 2);
  });

  it('does not shift while following the live edge (new items show at top)', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: true,
      resetKey: 'k',
    });

    rerender({
      headId: 'n1',
      renderedSegments: [seg('n1'), seg('a'), seg('b')],
      followingLiveEdge: true,
      resetKey: 'k',
    });

    expect(result.current).toBe(VIRTUOSO_START_INDEX);
  });

  it('does not shift when the head is unchanged (older items appended)', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    rerender({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b'), seg('c')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    expect(result.current).toBe(VIRTUOSO_START_INDEX);
  });

  it('finds the prior head inside a consolidated bundle', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    // Prepend a newer clip; the prior head 'a' is now bundled at index 1.
    rerender({
      headId: 'n1',
      renderedSegments: [seg('n1'), bundle('bundle-1', ['a', 'b'])],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    expect(result.current).toBe(VIRTUOSO_START_INDEX - 1);
  });

  it('resets to the base index when the query identity (resetKey) changes', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    rerender({
      headId: 'n1',
      renderedSegments: [seg('n1'), seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });
    expect(result.current).toBe(VIRTUOSO_START_INDEX - 1);

    // New feed/filter → wholesale replace → baseline resets.
    rerender({
      headId: 'z',
      renderedSegments: [seg('z')],
      followingLiveEdge: false,
      resetKey: 'k2',
    });
    expect(result.current).toBe(VIRTUOSO_START_INDEX);
  });

  it('resets cleanly even when the prior head survives into the new list', () => {
    const { result, rerender } = setup({
      headId: 'a',
      renderedSegments: [seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k',
    });

    // A wholesale replace whose head differs, but the prior head 'a' still
    // appears at index 1. The reset must win, not a spurious shift by 1.
    rerender({
      headId: 'x',
      renderedSegments: [seg('x'), seg('a'), seg('b')],
      followingLiveEdge: false,
      resetKey: 'k2',
    });
    expect(result.current).toBe(VIRTUOSO_START_INDEX);
  });
});
