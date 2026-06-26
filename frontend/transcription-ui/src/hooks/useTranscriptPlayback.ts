import { type RefObject } from 'react';
import type { VirtuosoHandle } from 'react-virtuoso';

import { type AudioSegment } from '@transcription/common';

import { type PlaybackController } from '../audio/WebAudioPlayer';
import {
  calculateSkipTimeDestination,
  findAdjacentSpeechSegment,
  findNextAudioSegment,
  getSegmentDuration,
  isWithinSegment,
} from '../utils/playbackUtils';
import { type RenderableAudioSegment } from './useConsolidatedAudioSegments';

interface UseTranscriptPlaybackProps {
  rawAudioSegments: AudioSegment[];
  audioSegments: RenderableAudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  isAudioPlaying: boolean;
  currentAudioRef: RefObject<PlaybackController | null>;
  virtuosoRef: RefObject<VirtuosoHandle | null>;
  toggleAudio: (id: string, uri: string) => void;
  onSeek: () => void;
}

export function useTranscriptPlayback({
  rawAudioSegments,
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  isAudioPlaying,
  currentAudioRef,
  virtuosoRef,
  toggleAudio,
  onSeek,
}: UseTranscriptPlaybackProps) {
  const getTargetId = () => {
    return isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
          currentlyPlayingSegmentId ||
          audioSegments[0]?.id;
  };

  const scrollSegmentIntoView = (segmentId: string) => {
    const displayIdx = audioSegments.findIndex((t) =>
      isWithinSegment(t, segmentId)
    );
    if (displayIdx !== -1) {
      virtuosoRef.current?.scrollToIndex({
        index: displayIdx,
        align: 'center',
        behavior: 'smooth',
      });
    }
  };

  const skipToNext = () => {
    const targetId = getTargetId();
    if (!targetId) return;

    const next = findNextAudioSegment(rawAudioSegments, targetId, 'forward');
    if (next) {
      toggleAudio(next.id, next.uri);
      scrollSegmentIntoView(next.id);
    }
  };

  const skipToPrevious = () => {
    const targetId = getTargetId();
    if (!targetId) return;

    const prev = findNextAudioSegment(rawAudioSegments, targetId, 'backward');
    if (prev) {
      toggleAudio(prev.id, prev.uri);
      scrollSegmentIntoView(prev.id);
    }
  };

  const skipToNextSpeech = () => {
    const targetId = getTargetId();
    if (!targetId) return;

    const next = findAdjacentSpeechSegment(audioSegments, targetId, 'forward');
    if (next) {
      toggleAudio(next.id, next.uri);
      virtuosoRef.current?.scrollToIndex({
        index: next.index,
        align: 'center',
        behavior: 'smooth',
      });
    }
  };

  const skipToPreviousSpeech = () => {
    const targetId = getTargetId();
    if (!targetId) return;

    const prev = findAdjacentSpeechSegment(audioSegments, targetId, 'backward');
    if (prev) {
      toggleAudio(prev.id, prev.uri);
      virtuosoRef.current?.scrollToIndex({
        index: prev.index,
        align: 'center',
        behavior: 'smooth',
      });
    }
  };

  const skipTime = (offsetSeconds: number) => {
    const activeId = currentlyPlayingSegmentId || highlightedSegmentId;
    if (!activeId) return;

    // Determine the baseline current time.
    // If a segment was active but the player is unloaded (e.g. track ended),
    // we are conceptually at the end of that segment, so we start from its duration.
    let currentTime = 0;
    const activeSegment = rawAudioSegments.find((s) => s.id === activeId);

    if (currentAudioRef.current) {
      currentTime = currentAudioRef.current.getCurrentTime();
    } else if (activeSegment) {
      currentTime = getSegmentDuration(activeSegment);
    }

    const dest = calculateSkipTimeDestination(
      rawAudioSegments,
      activeId,
      currentTime,
      offsetSeconds
    );

    if (dest) {
      const needToLoad =
        dest.segmentId !== currentlyPlayingSegmentId ||
        currentAudioRef.current === null;

      if (needToLoad) {
        toggleAudio(dest.segmentId, dest.playbackAudioUri);
      }
      currentAudioRef.current?.setCurrentTime(dest.seekTime);
      scrollSegmentIntoView(dest.segmentId);
      onSeek();
    }
  };

  return {
    skipToNext,
    skipToPrevious,
    skipToNextSpeech,
    skipToPreviousSpeech,
    skipTime,
  };
}
