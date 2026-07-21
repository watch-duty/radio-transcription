// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, cleanup, renderHook } from '@testing-library/react';

import { feedPlaybackView, useScannerPlayback } from './ScannerPlaybackContext';

type Rendered = { current: ReturnType<typeof useScannerPlayback> };

// A stand-in feed: mock play/stop handlers registered with the coordinator.
const makeFeed = () => ({
  onGranted: vi.fn(),
  onEvict: vi.fn(),
  play: vi.fn(),
  pause: vi.fn(),
});

const render = () => renderHook(() => useScannerPlayback());

const viewOf = (result: Rendered, feedId: string) =>
  feedPlaybackView(result.current.state, feedId);

// Register a feed and return its handlers; registration mutates state so it runs
// inside act.
function registerFeed(
  result: Rendered,
  feedId: string
): ReturnType<typeof makeFeed> {
  const handlers = makeFeed();
  act(() => {
    result.current.coordinator.register(feedId, handlers);
  });
  return handlers;
}

describe('useScannerPlayback', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    cleanup();
  });

  it('allows every feed to play when no mode is active', () => {
    const { result } = render();
    registerFeed(result, 'a');
    registerFeed(result, 'b');

    let aAllowed = false;
    let bAllowed = false;
    act(() => {
      aAllowed = result.current.coordinator.requestPlayback('a', false);
      bAllowed = result.current.coordinator.requestPlayback('b', false);
    });

    expect(aAllowed).toBe(true);
    expect(bAllowed).toBe(true);
    expect(viewOf(result, 'a').status).toBe('idle');
  });

  it('exposes a stable coordinator identity across state changes', () => {
    const { result } = render();
    const before = result.current.coordinator;
    registerFeed(result, 'a');
    act(() => result.current.coordinator.notifyAudible('a', true));

    // State changed (a is now playing) but the coordinator object did not — this
    // is what keeps consumers' register/report effects from looping.
    expect(result.current.state.anyPlaying).toBe(true);
    expect(result.current.coordinator).toBe(before);
  });

  describe('sequential mode', () => {
    it('grants the lock to the first requester and queues the rest', () => {
      const { result } = render();
      registerFeed(result, 'a');
      registerFeed(result, 'b');
      act(() => result.current.coordinator.setSequentialEnabled(true));

      let aAllowed = false;
      let bAllowed = false;
      act(() => {
        aAllowed = result.current.coordinator.requestPlayback('a', false);
        bAllowed = result.current.coordinator.requestPlayback('b', false);
      });

      expect(aAllowed).toBe(true);
      expect(bAllowed).toBe(false);
      expect(viewOf(result, 'b').status).toBe('queued');
      expect(viewOf(result, 'b').queuePosition).toBe(1);
    });

    it('grants the queued feed when the holder stops', () => {
      const { result } = render();
      registerFeed(result, 'a');
      const b = registerFeed(result, 'b');
      act(() => result.current.coordinator.setSequentialEnabled(true));

      act(() => {
        result.current.coordinator.requestPlayback('a', false);
        result.current.coordinator.notifyAudible('a', true);
        result.current.coordinator.requestPlayback('b', false);
      });
      expect(b.onGranted).not.toHaveBeenCalled();

      act(() => result.current.coordinator.notifyAudible('a', false));

      expect(b.onGranted).toHaveBeenCalledTimes(1);
    });

    it('lets a user-initiated play take over, evicting the current holder', () => {
      const { result } = render();
      const a = registerFeed(result, 'a');
      registerFeed(result, 'b');
      act(() => result.current.coordinator.setSequentialEnabled(true));

      act(() => {
        result.current.coordinator.requestPlayback('a', false);
        result.current.coordinator.notifyAudible('a', true);
      });
      // Ignore the evict from enabling the mode; assert only the takeover evict.
      a.onEvict.mockClear();

      let bAllowed = false;
      act(() => {
        bAllowed = result.current.coordinator.requestPlayback('b', true);
      });

      expect(bAllowed).toBe(true);
      expect(a.onEvict).toHaveBeenCalledTimes(1);
    });

    it('passes the lock onward when a granted feed relinquishes', () => {
      const { result } = render();
      registerFeed(result, 'a');
      const b = registerFeed(result, 'b');
      const c = registerFeed(result, 'c');
      act(() => result.current.coordinator.setSequentialEnabled(true));

      act(() => {
        result.current.coordinator.requestPlayback('a', false);
        result.current.coordinator.notifyAudible('a', true);
        result.current.coordinator.requestPlayback('b', false);
        result.current.coordinator.requestPlayback('c', false);
      });

      // a stops → b is granted; b can't actually play and relinquishes → c.
      act(() => result.current.coordinator.notifyAudible('a', false));
      expect(b.onGranted).toHaveBeenCalledTimes(1);

      act(() => result.current.coordinator.relinquish('b'));
      expect(c.onGranted).toHaveBeenCalledTimes(1);
    });

    it('evicts everyone when the mode is enabled', () => {
      const { result } = render();
      const a = registerFeed(result, 'a');
      const b = registerFeed(result, 'b');

      act(() => {
        result.current.coordinator.notifyAudible('a', true);
        result.current.coordinator.notifyAudible('b', true);
      });

      act(() => result.current.coordinator.setSequentialEnabled(true));

      expect(a.onEvict).toHaveBeenCalledTimes(1);
      expect(b.onEvict).toHaveBeenCalledTimes(1);
    });
  });

  describe('priority', () => {
    it('interrupts other feeds while the starred feed plays', () => {
      const { result } = render();
      registerFeed(result, 'a');
      const b = registerFeed(result, 'b');

      act(() => {
        result.current.coordinator.notifyAudible('b', true);
        result.current.coordinator.togglePriority('a');
        result.current.coordinator.notifyAudible('a', true);
      });

      expect(b.onEvict).toHaveBeenCalledTimes(1);
      expect(viewOf(result, 'b').status).toBe('interrupted');
    });

    it('resumes interrupted feeds when the starred feed stops', () => {
      const { result } = render();
      registerFeed(result, 'a');
      const b = registerFeed(result, 'b');

      act(() => {
        result.current.coordinator.notifyAudible('b', true);
        result.current.coordinator.togglePriority('a');
        result.current.coordinator.notifyAudible('a', true);
      });
      act(() => result.current.coordinator.notifyAudible('a', false));

      expect(b.onGranted).toHaveBeenCalledTimes(1);
    });

    it('ducks a non-priority feed that requests while priority is audible', () => {
      const { result } = render();
      registerFeed(result, 'a');
      registerFeed(result, 'b');

      act(() => {
        result.current.coordinator.togglePriority('a');
        result.current.coordinator.notifyAudible('a', true);
      });

      let bAllowed = true;
      act(() => {
        bAllowed = result.current.coordinator.requestPlayback('b', true);
      });

      expect(bAllowed).toBe(false);
    });

    it('is single-select — starring another feed replaces the first', () => {
      const { result } = render();
      registerFeed(result, 'a');
      registerFeed(result, 'b');

      act(() => result.current.coordinator.togglePriority('a'));
      act(() => result.current.coordinator.togglePriority('b'));

      expect(viewOf(result, 'a').starred).toBe(false);
      expect(viewOf(result, 'b').starred).toBe(true);
    });
  });

  describe('solo', () => {
    it('mutes every other feed (gain only, no eviction)', () => {
      const { result } = render();
      registerFeed(result, 'a');
      const b = registerFeed(result, 'b');

      act(() => result.current.coordinator.toggleSolo('a'));

      expect(viewOf(result, 'a').soloed).toBe(true);
      expect(viewOf(result, 'a').muted).toBe(false);
      expect(viewOf(result, 'b').muted).toBe(true);
      expect(b.onEvict).not.toHaveBeenCalled();
    });

    it('is single-select and toggles off', () => {
      const { result } = render();
      registerFeed(result, 'a');
      registerFeed(result, 'b');

      act(() => result.current.coordinator.toggleSolo('a'));
      act(() => result.current.coordinator.toggleSolo('b'));
      expect(viewOf(result, 'a').soloed).toBe(false);
      expect(viewOf(result, 'b').soloed).toBe(true);

      act(() => result.current.coordinator.toggleSolo('b'));
      expect(viewOf(result, 'b').soloed).toBe(false);
      expect(viewOf(result, 'a').muted).toBe(false);
    });
  });

  describe('bulk actions', () => {
    it('play all triggers every feed play; pause all triggers every pause', () => {
      const { result } = render();
      const a = registerFeed(result, 'a');
      const b = registerFeed(result, 'b');

      act(() => result.current.coordinator.playAll());
      expect(a.play).toHaveBeenCalledTimes(1);
      expect(b.play).toHaveBeenCalledTimes(1);

      act(() => result.current.coordinator.pauseAll());
      expect(a.pause).toHaveBeenCalledTimes(1);
      expect(b.pause).toHaveBeenCalledTimes(1);
    });

    it('mute all mutes every feed and unmutes on clear', () => {
      const { result } = render();
      registerFeed(result, 'a');
      registerFeed(result, 'b');

      act(() => result.current.coordinator.setMuteAll(true));
      expect(viewOf(result, 'a').muted).toBe(true);
      expect(viewOf(result, 'b').muted).toBe(true);

      act(() => result.current.coordinator.setMuteAll(false));
      expect(viewOf(result, 'a').muted).toBe(false);
    });
  });

  it('reports whether any feed is audible', () => {
    const { result } = render();
    registerFeed(result, 'a');

    expect(result.current.state.anyPlaying).toBe(false);
    act(() => result.current.coordinator.notifyAudible('a', true));
    expect(result.current.state.anyPlaying).toBe(true);
    act(() => result.current.coordinator.notifyAudible('a', false));
    expect(result.current.state.anyPlaying).toBe(false);
  });

  it('advances the queue when the lock holder unregisters', () => {
    const { result } = render();
    let unregisterA = () => {};
    const aHandlers = makeFeed();
    act(() => {
      unregisterA = result.current.coordinator.register('a', aHandlers);
    });
    const b = registerFeed(result, 'b');
    act(() => result.current.coordinator.setSequentialEnabled(true));

    act(() => {
      result.current.coordinator.requestPlayback('a', false);
      result.current.coordinator.notifyAudible('a', true);
      result.current.coordinator.requestPlayback('b', false);
    });

    act(() => unregisterA());

    expect(b.onGranted).toHaveBeenCalledTimes(1);
  });
});
