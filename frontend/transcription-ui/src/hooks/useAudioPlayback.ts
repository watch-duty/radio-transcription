import {
  type RefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from 'react';

import { type AudioSegment } from '@transcription/common';

import {
  type PlaybackController,
  WebAudioPlayer,
  createAudioContext,
} from '../audio/WebAudioPlayer';
import { getAudioUrl } from '../utils/audioUtils';
import { getNextContinuousSegment } from '../utils/playbackUtils';
import { type RenderableAudioSegment } from './useConsolidatedAudioSegments';

interface UseAudioPlaybackParams {
  // The latest consolidated audio segments, read inside the `onEnd` callback so continuous
  // playback always evaluates against the current list.
  audioSegmentsRef: RefObject<RenderableAudioSegment[]>;
  // The latest raw audio segments, used to find segments within bundles.
  rawAudioSegmentsRef: RefObject<AudioSegment[]>;
  // Called when a new segment starts so the view can highlight it.
  onPlaySegment: (segmentId: string) => void;
  volumeDb: number;
  pan: number;
  speed: number;
  onPlaybackEnded?: (lastSegmentId: string) => void;
}

interface UseAudioPlayback {
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  currentAudioRef: RefObject<PlaybackController | null>;
  togglePlay: (
    segmentId: string,
    audioUri: string,
    initialSeekTime?: number
  ) => void;
  stop: () => void;
}

// Owns the Web Audio engine plumbing — the shared AudioContext, the player, and
// the per-segment playback handle — plus the play/pause state. The view keeps
// the transcript-domain concerns (which segment to advance to, highlighting).
export function useAudioPlayback({
  audioSegmentsRef,
  rawAudioSegmentsRef,
  onPlaySegment,
  volumeDb,
  pan,
  speed,
  onPlaybackEnded,
}: UseAudioPlaybackParams): UseAudioPlayback {
  const audioContextRef = useRef<AudioContext | null>(null);
  const playerRef = useRef<WebAudioPlayer | null>(null);
  const currentAudio = useRef<PlaybackController | null>(null);

  // Refs keep these out of `togglePlay`'s deps so its identity stays stable.
  const volumeDbRef = useRef(volumeDb);
  const panRef = useRef(pan);
  const speedRef = useRef(speed);
  const onPlaybackEndedRef = useRef(onPlaybackEnded);

  // Push control changes into the engine; the player is created lazily on first
  // play, so before then these only update the refs that `togglePlay` reads.
  useEffect(() => {
    volumeDbRef.current = volumeDb;
    playerRef.current?.setVolumeDb(volumeDb);
  }, [volumeDb]);
  useEffect(() => {
    panRef.current = pan;
    playerRef.current?.setPan(pan);
  }, [pan]);
  useEffect(() => {
    speedRef.current = speed;
    playerRef.current?.setSpeed(speed);
  }, [speed]);
  useEffect(() => {
    onPlaybackEndedRef.current = onPlaybackEnded;
  }, [onPlaybackEnded]);

  const [currentlyPlayingSegmentId, setCurrentlyPlayingSegmentId] = useState<
    string | null
  >(null);
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  // Mutable refs to keep state fresh and avoid stale closures in audio event listeners
  const currentlyPlayingSegmentIdRef = useRef<string | null>(null);
  const isAudioPlayingRef = useRef(false);

  useEffect(() => {
    currentlyPlayingSegmentIdRef.current = currentlyPlayingSegmentId;
  }, [currentlyPlayingSegmentId]);

  useEffect(() => {
    isAudioPlayingRef.current = isAudioPlaying;
  }, [isAudioPlaying]);

  useEffect(() => {
    return () => {
      audioContextRef.current?.close().catch(() => {});
      audioContextRef.current = null;
      playerRef.current = null;
      currentAudio.current = null;
    };
  }, []);

  const togglePlay = useCallback(
    (segmentId: string, audioUri: string, initialSeekTime?: number) => {
      // Lazy-build on first play so the AudioContext is created inside a user gesture.
      const context = (audioContextRef.current ??= createAudioContext());
      const player = (playerRef.current ??= new WebAudioPlayer(context));
      player.resume();
      player.setVolumeDb(volumeDbRef.current);
      player.setPan(panRef.current);
      player.setSpeed(speedRef.current);

      const newAudio = currentlyPlayingSegmentIdRef.current !== segmentId;

      if (newAudio) {
        currentAudio.current?.unload();
        currentAudio.current = null;
        setCurrentlyPlayingSegmentId(segmentId);
        currentlyPlayingSegmentIdRef.current = segmentId;
        onPlaySegment(segmentId);
      }

      if (!currentAudio.current) {
        currentAudio.current = player.load(getAudioUrl(audioUri), {
          onPlay: () => {
            setIsAudioPlaying(true);
            isAudioPlayingRef.current = true;
          },
          onPause: () => {
            setIsAudioPlaying(false);
            isAudioPlayingRef.current = false;
          },
          onError: () => {
            setIsAudioPlaying(false);
            isAudioPlayingRef.current = false;
          },
          onEnd: () => {
            const next = getNextContinuousSegment(
              audioSegmentsRef.current ?? [],
              rawAudioSegmentsRef.current ?? [],
              segmentId
            );

            if (next) {
              // Transition and play the next segment synchronously to bypass background tab throttling
              togglePlay(next.id, next.uri);
            } else {
              setIsAudioPlaying(false);
              isAudioPlayingRef.current = false;
              currentAudio.current = null;
              onPlaybackEndedRef.current?.(segmentId);
            }
          },
        });
      }

      if (initialSeekTime !== undefined) {
        currentAudio.current.setCurrentTime(initialSeekTime);
      }

      if (
        !isAudioPlayingRef.current ||
        newAudio ||
        initialSeekTime !== undefined
      ) {
        currentAudio.current.play();
      } else {
        currentAudio.current.pause();
      }
    },
    [audioSegmentsRef, rawAudioSegmentsRef, onPlaySegment]
  );

  const stop = useCallback(() => {
    playerRef.current?.stop();
    currentAudio.current = null;
    setCurrentlyPlayingSegmentId(null);
    currentlyPlayingSegmentIdRef.current = null;
    // stop() tears down the listeners, so the engine's async `pause` event never
    // reaches onPause — own the transition here so playback state clears.
    setIsAudioPlaying(false);
    isAudioPlayingRef.current = false;
  }, []);

  return {
    isAudioPlaying,
    currentlyPlayingSegmentId,
    currentAudioRef: currentAudio,
    togglePlay,
    stop,
  };
}
