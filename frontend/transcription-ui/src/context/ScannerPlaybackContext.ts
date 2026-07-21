import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

export type FeedPlaybackStatus = 'playing' | 'queued' | 'interrupted' | 'idle';

export interface FeedPlaybackView {
  status: FeedPlaybackStatus;
  queuePosition: number | null;
  muted: boolean;
  starred: boolean;
  soloed: boolean;
}

const IDLE_VIEW: FeedPlaybackView = {
  status: 'idle',
  queuePosition: null,
  muted: false,
  starred: false,
  soloed: false,
};

interface FeedHandlers {
  // Start playback (from the latest clip) because the coordinator granted the
  // lock or resumed this feed after an interrupt.
  onGranted: () => void;
  // Stop playback because another feed took over (sequential takeover, priority
  // interrupt, or enabling sequential mode).
  onEvict: () => void;
  // Bulk "Play all": express intent-to-play and attempt a gated start (queues
  // under sequential mode / ducks under priority, like a live-speech autoplay).
  play: () => void;
  // Bulk "Pause all": drop intent-to-play and stop.
  pause: () => void;
}

// The imperative coordinator. Its identity is STABLE across renders so effects
// that register / report to it don't re-run (and loop) on every state change.
export interface ScannerPlaybackCoordinator {
  register: (feedId: string, handlers: FeedHandlers) => () => void;
  // Gate a feed's own play attempt. Returns false if the feed must wait (queued
  // behind the sequential lock, or ducked while a priority feed is playing).
  // `userInitiated` force-acquires, evicting the current holder.
  requestPlayback: (feedId: string, userInitiated: boolean) => boolean;
  // Report whether the feed is actually emitting audio right now.
  notifyAudible: (feedId: string, isAudible: boolean) => void;
  // A granted feed can't actually start (e.g. the user paused it while queued);
  // pass the sequential lock to the next queued feed.
  relinquish: (feedId: string) => void;
  setSequentialEnabled: (value: boolean) => void;
  togglePriority: (feedId: string) => void;
  toggleSolo: (feedId: string) => void;
  // Bulk actions from the scanner toolbar.
  playAll: () => void;
  pauseAll: () => void;
  setMuteAll: (value: boolean) => void;
}

// The reactive snapshot for rendering (badges, star/solo/muted, checkbox). This
// changes identity on every coordination change — read it, don't depend on it
// in effects.
export interface ScannerPlaybackState {
  sequentialEnabled: boolean;
  muteAll: boolean;
  anyPlaying: boolean;
  priorityFeedId: string | null;
  soloFeedId: string | null;
  statusByFeed: Record<
    string,
    { status: FeedPlaybackStatus; queuePosition: number | null }
  >;
}

export const ScannerPlaybackContext =
  createContext<ScannerPlaybackCoordinator | null>(null);

export const ScannerPlaybackStateContext =
  createContext<ScannerPlaybackState | null>(null);

export const useScannerPlaybackCoordinator =
  (): ScannerPlaybackCoordinator | null => useContext(ScannerPlaybackContext);

export const useScannerPlaybackState = (): ScannerPlaybackState | null =>
  useContext(ScannerPlaybackStateContext);

// Derive a single feed's view from the reactive state.
export function feedPlaybackView(
  state: ScannerPlaybackState | null,
  feedId: string
): FeedPlaybackView {
  if (!state) return IDLE_VIEW;
  const entry = state.statusByFeed[feedId];
  return {
    status: entry?.status ?? 'idle',
    queuePosition: entry?.queuePosition ?? null,
    muted:
      state.muteAll ||
      (state.soloFeedId !== null && state.soloFeedId !== feedId),
    starred: state.priorityFeedId === feedId,
    soloed: state.soloFeedId === feedId,
  };
}

/**
 * Cross-panel playback coordinator for the scanner, porting FNPlayer-Timeline's
 * sequential lock + priority interrupt + solo model:
 *
 * - Sequential mode: one feed holds the lock and plays; others queue and are
 *   granted the lock in turn as it releases.
 * - Priority (single starred feed): while it plays, all others are interrupted
 *   (paused) and resume when it stops.
 * - Solo (single feed): every other feed is muted (gain only, still playing).
 *
 * With no mode active, feeds play concurrently — the default scanner behavior.
 * Instantiated once by the scanner view and shared with panels via context; the
 * feed page has no provider, so every feed there is unaffected.
 */
export function useScannerPlayback(): {
  coordinator: ScannerPlaybackCoordinator;
  state: ScannerPlaybackState;
} {
  // Session-scoped so it always starts off; both are coordination modes, not
  // remembered preferences (a global setting can own the default later).
  const [sequentialEnabled, setSequentialState] = useState(false);
  const [muteAll, setMuteAllState] = useState(false);
  // Mirror the mode into a ref for synchronous reads inside the coordinator's
  // event handlers (which mustn't close over the render-time value).
  const sequentialRef = useRef(sequentialEnabled);
  useEffect(() => {
    sequentialRef.current = sequentialEnabled;
  }, [sequentialEnabled]);

  const handlersRef = useRef(new Map<string, FeedHandlers>());
  const lockHolderRef = useRef<string | null>(null);
  const queueRef = useRef<string[]>([]);
  const playingRef = useRef(new Set<string>());
  const interruptedRef = useRef(new Set<string>());
  const priorityRef = useRef<string | null>(null);
  const soloRef = useRef<string | null>(null);

  // Reactive mirror consumed by panels for badges / muted / star / solo state.
  const [snapshot, setSnapshot] = useState<{
    priorityFeedId: string | null;
    soloFeedId: string | null;
    anyPlaying: boolean;
    statusByFeed: Record<
      string,
      { status: FeedPlaybackStatus; queuePosition: number | null }
    >;
  }>({
    priorityFeedId: null,
    soloFeedId: null,
    anyPlaying: false,
    statusByFeed: {},
  });

  const recompute = useCallback(() => {
    const statusByFeed: Record<
      string,
      { status: FeedPlaybackStatus; queuePosition: number | null }
    > = {};
    for (const feedId of handlersRef.current.keys()) {
      const queueIndex = queueRef.current.indexOf(feedId);
      const status: FeedPlaybackStatus = playingRef.current.has(feedId)
        ? 'playing'
        : interruptedRef.current.has(feedId)
          ? 'interrupted'
          : queueIndex >= 0
            ? 'queued'
            : 'idle';
      statusByFeed[feedId] = {
        status,
        queuePosition: queueIndex >= 0 ? queueIndex + 1 : null,
      };
    }
    setSnapshot({
      priorityFeedId: priorityRef.current,
      soloFeedId: soloRef.current,
      anyPlaying: playingRef.current.size > 0,
      statusByFeed,
    });
  }, []);

  // Stop a feed and treat it as no-longer-playing immediately, rather than
  // waiting for its async notifyAudible(false) — so status/lock transitions are
  // synchronous. The feed's later notifyAudible(false) is then a no-op.
  const stopFeed = useCallback((feedId: string) => {
    playingRef.current.delete(feedId);
    handlersRef.current.get(feedId)?.onEvict();
  }, []);

  // Grant the lock to the next queued feed; it relinquishes if it can't actually
  // start, falling through to the next.
  const advanceQueue = useCallback(() => {
    while (queueRef.current.length > 0) {
      const nextFeedId = queueRef.current.shift()!;
      const handlers = handlersRef.current.get(nextFeedId);
      if (!handlers) continue;
      lockHolderRef.current = nextFeedId;
      handlers.onGranted();
      return;
    }
    lockHolderRef.current = null;
  }, []);

  // A priority feed is playing → pause every other currently-audible feed and
  // remember it so it resumes when priority clears.
  const interruptOthersFor = useCallback(
    (priorityFeedId: string) => {
      for (const feedId of handlersRef.current.keys()) {
        if (feedId === priorityFeedId) continue;
        if (!playingRef.current.has(feedId)) continue;
        interruptedRef.current.add(feedId);
        stopFeed(feedId);
      }
    },
    [stopFeed]
  );

  // Priority cleared (stopped or unstarred) → resume interrupted feeds. Under
  // sequential mode only one may resume; the rest go back into the queue.
  const resumeInterrupted = useCallback(() => {
    const interrupted = [...interruptedRef.current];
    interruptedRef.current.clear();
    if (interrupted.length === 0) return;

    if (sequentialRef.current) {
      const [first, ...rest] = interrupted;
      lockHolderRef.current = first;
      queueRef.current = [
        ...rest.filter((k) => !queueRef.current.includes(k)),
        ...queueRef.current,
      ];
      handlersRef.current.get(first)?.onGranted();
    } else {
      for (const feedId of interrupted) {
        handlersRef.current.get(feedId)?.onGranted();
      }
    }
  }, []);

  const register = useCallback(
    (feedId: string, handlers: FeedHandlers) => {
      handlersRef.current.set(feedId, handlers);
      recompute();
      return () => {
        handlersRef.current.delete(feedId);
        playingRef.current.delete(feedId);
        interruptedRef.current.delete(feedId);
        queueRef.current = queueRef.current.filter((k) => k !== feedId);
        if (lockHolderRef.current === feedId) advanceQueue();
        if (priorityRef.current === feedId) {
          priorityRef.current = null;
          resumeInterrupted();
        }
        if (soloRef.current === feedId) soloRef.current = null;
        recompute();
      };
    },
    [advanceQueue, recompute, resumeInterrupted]
  );

  const requestPlayback = useCallback(
    (feedId: string, userInitiated: boolean) => {
      const priorityFeedId = priorityRef.current;

      // Priority feed always plays and interrupts the rest.
      if (priorityFeedId === feedId) return true;

      // Ducked while a priority feed is audible; remember for resume.
      if (priorityFeedId !== null && playingRef.current.has(priorityFeedId)) {
        interruptedRef.current.add(feedId);
        recompute();
        return false;
      }

      if (!sequentialRef.current) return true;

      if (lockHolderRef.current === null || lockHolderRef.current === feedId) {
        lockHolderRef.current = feedId;
        return true;
      }
      if (userInitiated) {
        const previous = lockHolderRef.current;
        lockHolderRef.current = feedId;
        queueRef.current = queueRef.current.filter((k) => k !== feedId);
        stopFeed(previous);
        recompute();
        return true;
      }
      if (!queueRef.current.includes(feedId)) queueRef.current.push(feedId);
      recompute();
      return false;
    },
    [recompute, stopFeed]
  );

  const notifyAudible = useCallback(
    (feedId: string, isAudible: boolean) => {
      if (isAudible) {
        playingRef.current.add(feedId);
        interruptedRef.current.delete(feedId);
        if (priorityRef.current === feedId) interruptOthersFor(feedId);
      } else {
        const wasPlaying = playingRef.current.delete(feedId);
        if (wasPlaying) {
          if (priorityRef.current === feedId) {
            resumeInterrupted();
          } else if (
            sequentialRef.current &&
            lockHolderRef.current === feedId
          ) {
            advanceQueue();
          }
        }
      }
      recompute();
    },
    [advanceQueue, interruptOthersFor, recompute, resumeInterrupted]
  );

  const relinquish = useCallback(
    (feedId: string) => {
      interruptedRef.current.delete(feedId);
      if (lockHolderRef.current === feedId) advanceQueue();
      recompute();
    },
    [advanceQueue, recompute]
  );

  const setSequentialEnabled = useCallback(
    (value: boolean) => {
      setSequentialState(value);
      sequentialRef.current = value;
      queueRef.current = [];
      lockHolderRef.current = null;
      interruptedRef.current.clear();
      // Enabling drops us to a clean single-stream: stop everyone so the next
      // request (a live-speech autoplay or a click) takes the lock alone.
      if (value) {
        for (const feedId of handlersRef.current.keys()) stopFeed(feedId);
      }
      recompute();
    },
    [recompute, stopFeed]
  );

  const togglePriority = useCallback(
    (feedId: string) => {
      if (priorityRef.current === feedId) {
        priorityRef.current = null;
        resumeInterrupted();
      } else {
        priorityRef.current = feedId;
        if (playingRef.current.has(feedId)) interruptOthersFor(feedId);
      }
      recompute();
    },
    [interruptOthersFor, recompute, resumeInterrupted]
  );

  const toggleSolo = useCallback(
    (feedId: string) => {
      soloRef.current = soloRef.current === feedId ? null : feedId;
      recompute();
    },
    [recompute]
  );

  const playAll = useCallback(() => {
    // Each feed expresses intent and attempts a gated start, so sequential mode
    // still admits one and queues the rest, and priority still ducks the rest.
    for (const handlers of handlersRef.current.values()) handlers.play();
    recompute();
  }, [recompute]);

  const pauseAll = useCallback(() => {
    // Clear the lock/queue first so stopping feeds doesn't advance the queue.
    lockHolderRef.current = null;
    queueRef.current = [];
    interruptedRef.current.clear();
    for (const handlers of handlersRef.current.values()) handlers.pause();
    recompute();
  }, [recompute]);

  const setMuteAll = useCallback((value: boolean) => {
    setMuteAllState(value);
  }, []);

  // Stable — all deps are stable callbacks — so consumers' register/report
  // effects don't re-run on state changes.
  const coordinator = useMemo<ScannerPlaybackCoordinator>(
    () => ({
      register,
      requestPlayback,
      notifyAudible,
      relinquish,
      setSequentialEnabled,
      togglePriority,
      toggleSolo,
      playAll,
      pauseAll,
      setMuteAll,
    }),
    [
      register,
      requestPlayback,
      notifyAudible,
      relinquish,
      setSequentialEnabled,
      togglePriority,
      toggleSolo,
      playAll,
      pauseAll,
      setMuteAll,
    ]
  );

  const state = useMemo<ScannerPlaybackState>(
    () => ({
      sequentialEnabled,
      muteAll,
      anyPlaying: snapshot.anyPlaying,
      priorityFeedId: snapshot.priorityFeedId,
      soloFeedId: snapshot.soloFeedId,
      statusByFeed: snapshot.statusByFeed,
    }),
    [sequentialEnabled, muteAll, snapshot]
  );

  return { coordinator, state };
}
