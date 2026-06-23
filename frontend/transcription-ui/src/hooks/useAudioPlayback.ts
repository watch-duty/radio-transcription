import {
  type Dispatch,
  type RefObject,
  type SetStateAction,
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

interface UseAudioPlaybackParams {
  // The latest audio segments, read inside the `onEnd` callback so continuous
  // playback always evaluates against the current list.
  audioSegmentsRef: RefObject<AudioSegment[]>;
  // Called when a new segment starts so the view can highlight it.
  onPlaySegment: (segmentId: string) => void;
}

interface UseAudioPlayback {
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  playbackEndedForId: string | null;
  setPlaybackEndedForId: Dispatch<SetStateAction<string | null>>;
  currentAudioRef: RefObject<PlaybackController | null>;
  togglePlay: (segmentId: string, audioUri: string) => void;
  stop: () => void;
}

// Owns the Web Audio engine plumbing — the shared AudioContext, the player, and
// the per-segment playback handle — plus the play/pause state. The view keeps
// the transcript-domain concerns (which segment to advance to, highlighting).
export function useAudioPlayback({
  audioSegmentsRef,
  onPlaySegment,
}: UseAudioPlaybackParams): UseAudioPlayback {
  const audioContextRef = useRef<AudioContext | null>(null);
  const playerRef = useRef<WebAudioPlayer | null>(null);
  const currentAudio = useRef<PlaybackController | null>(null);

  const [currentlyPlayingSegmentId, setCurrentlyPlayingSegmentId] = useState<
    string | null
  >(null);
  const [playbackEndedForId, setPlaybackEndedForId] = useState<string | null>(
    null
  );
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  useEffect(() => {
    return () => {
      audioContextRef.current?.close().catch(() => {});
      audioContextRef.current = null;
      playerRef.current = null;
      currentAudio.current = null;
    };
  }, []);

  const togglePlay = useCallback(
    (segmentId: string, audioUri: string) => {
      // Lazy-build on first play so the AudioContext is created inside a user gesture.
      const context = (audioContextRef.current ??= createAudioContext());
      const player = (playerRef.current ??= new WebAudioPlayer(context));
      player.resume();

      const newAudio = currentlyPlayingSegmentId !== segmentId;

      if (newAudio) {
        currentAudio.current?.unload();
        currentAudio.current = null;
        setCurrentlyPlayingSegmentId(segmentId);
        onPlaySegment(segmentId);
      }

      if (!currentAudio.current) {
        currentAudio.current = player.load(getAudioUrl(audioUri), {
          onPlay: () => setIsAudioPlaying(true),
          onPause: () => setIsAudioPlaying(false),
          onError: () => setIsAudioPlaying(false),
          onEnd: () => {
            const currentAudioSegments = audioSegmentsRef.current;
            const currentIndex = currentAudioSegments.findIndex(
              (t) => t.id === segmentId
            );
            const hasNext = currentIndex > 0;

            if (!hasNext) {
              setIsAudioPlaying(false);
            }

            setPlaybackEndedForId(segmentId);
            currentAudio.current = null;
          },
        });
      }

      if (!isAudioPlaying || newAudio) {
        currentAudio.current.play();
      } else {
        currentAudio.current.pause();
      }
    },
    [currentlyPlayingSegmentId, isAudioPlaying, audioSegmentsRef, onPlaySegment]
  );

  const stop = useCallback(() => {
    playerRef.current?.stop();
    currentAudio.current = null;
    setCurrentlyPlayingSegmentId(null);
    setPlaybackEndedForId(null);
  }, []);

  return {
    isAudioPlaying,
    currentlyPlayingSegmentId,
    playbackEndedForId,
    setPlaybackEndedForId,
    currentAudioRef: currentAudio,
    togglePlay,
    stop,
  };
}
