import { useState } from 'react';

import { isWithinSegment } from '../utils/playbackUtils';
import { type RenderableAudioSegment } from './useConsolidatedAudioSegments';

// Lowered as items prepend so Virtuoso preserves scroll position; starts high to
// stay positive across many prepends.
export const VIRTUOSO_START_INDEX = 1_000_000;

interface UseScrollAnchorOptions {
  // Raw newest segment id — the anchor. Kept separate from the rendered head so
  // re-bundling (which can change the consolidated head) can't shift the scroll.
  headId: string | null;
  // The consolidated list Virtuoso renders.
  renderedSegments: RenderableAudioSegment[];
  // While following the live edge, new items should surface at the top rather
  // than anchor the prior view in place.
  followingLiveEdge: boolean;
  // Identity of the current query; a change replaces the list wholesale.
  resetKey: string;
}

// Keeps Virtuoso's scroll position stable when newer segments prepend from any
// source (live poll, load-newer, window preload), except while following the
// live edge (where new items should appear at the top).
export function useScrollAnchor({
  headId,
  renderedSegments,
  followingLiveEdge,
  resetKey,
}: UseScrollAnchorOptions): number {
  const [firstItemIndex, setFirstItemIndex] = useState(VIRTUOSO_START_INDEX);
  const [prevResetKey, setPrevResetKey] = useState(resetKey);
  const [prevHeadId, setPrevHeadId] = useState<string | null>(null);

  // Adjust during render (not an effect) so the prepended data and the corrected
  // index land in one commit.
  if (resetKey !== prevResetKey) {
    // A new query replaces the list wholesale, so reset the baseline. Syncing
    // prevHeadId keeps the head-change branch from clobbering that reset.
    setPrevResetKey(resetKey);
    setPrevHeadId(headId);
    setFirstItemIndex(VIRTUOSO_START_INDEX);
  } else if (headId !== prevHeadId) {
    setPrevHeadId(headId);
    if (prevHeadId !== null) {
      // Lower firstItemIndex by the count of rendered items inserted above the
      // prior head. > 0 means a prepend; -1 (not found) and 0 (unchanged) no-op.
      const prepended = renderedSegments.findIndex((segment) =>
        isWithinSegment(segment, prevHeadId)
      );
      if (prepended > 0 && !followingLiveEdge) {
        setFirstItemIndex((index) => index - prepended);
      }
    }
  }

  return firstItemIndex;
}
