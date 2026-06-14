// @vitest-environment jsdom
import React from 'react';

import type { Howl } from 'howler';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import { getAudioUrl } from '../../utils/audioUtils';
import { DEFAULT_AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { AudioDisplay } from './AudioDisplay';
import {
  __primePeaksCacheForTest,
  __resetPeaksCacheForTest,
} from './usePeaksDecodeQueue';

const mockSetTime = vi.fn();
const mockSetOptions = vi.fn();

// Display-only: the player renders from cached peaks/duration, not a url.
vi.mock('@wavesurfer/react', () => {
  const MockWavesurferPlayer = (props: {
    peaks?: (number[] | Float32Array)[];
    duration?: number;
    onReady?: (ws: {
      setTime: (time: number) => void;
      setOptions: (opts: unknown) => void;
    }) => void;
    onDestroy?: () => void;
  }) => {
    const { onReady, onDestroy } = props;
    React.useEffect(() => {
      onReady?.({ setTime: mockSetTime, setOptions: mockSetOptions });
      return () => onDestroy?.();
    }, [onReady, onDestroy]);

    return (
      <div
        data-testid="wavesurfer-player"
        data-peaks-len={props.peaks?.[0]?.length}
        data-duration={props.duration}
      />
    );
  };

  return { default: MockWavesurferPlayer };
});

// The headless decode queue creates a real WaveSurfer; stub it so jsdom (no
// audio decoding) doesn't try to fetch/decode. Tests prime the cache instead.
vi.mock('wavesurfer.js', () => ({
  default: {
    create: () => ({
      on: () => {},
      destroy: () => {},
      exportPeaks: () => [[]],
      getDuration: () => 0,
    }),
  },
}));

function makeMockAudioSegment(
  id: string,
  startTimestamp: string,
  endTimestamp: string,
  playbackAudioUri: string,
  evaluationDecisions: string[] = []
): AudioSegment {
  return {
    id,
    feedId: 'feed1',
    classification: AudioClassification.SPEECH,
    startTimestamp,
    endTimestamp,
    canonicalAudioUri: 'gs://bucket/audio.flac',
    playbackAudioUri,
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: startTimestamp,
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: startTimestamp,
        data: { text: 'text', errors: [] },
      },
      ...(evaluationDecisions.length > 0
        ? [
            {
              type: AnnotationType.EVALUATION,
              createdAt: startTimestamp,
              data: { decisions: evaluationDecisions, errors: [] },
            },
          ]
        : []),
    ],
  };
}

// AudioDisplay is controlled by useAudioTimelineWindow (tested separately). These
// tests cover its rendering + playback cursor; supply a live (null-end) window
// wide enough to show the segments under test.
const renderDisplay = (
  overrides: Partial<React.ComponentProps<typeof AudioDisplay>>
) =>
  render(
    <AudioDisplay
      audioSegments={[]}
      currentlyPlayingSegmentId={null}
      highlightedSegmentId={null}
      onClipClick={vi.fn()}
      isAudioPlaying={false}
      onTogglePlayPause={vi.fn()}
      windowEndTime={null}
      windowDurationMs={DEFAULT_AUDIO_WINDOW_DURATION_MS}
      rangeStartMs={null}
      maxEnd={null}
      miniMapTimes={[]}
      onScrubToCenter={vi.fn()}
      {...overrides}
    />
  );

const seg = (id: string, hhmm: string, uri = `${id}.m4a`) =>
  makeMockAudioSegment(
    id,
    new Date(`2026-04-20T${hhmm}:00Z`).toISOString(),
    new Date(`2026-04-20T${hhmm}:05Z`).toISOString(),
    uri
  );

describe('AudioDisplay', () => {
  afterEach(() => {
    cleanup();
    __resetPeaksCacheForTest();
    mockSetTime.mockClear();
    mockSetOptions.mockClear();
  });

  it('renders the empty state when there are no segments', () => {
    renderDisplay({ audioSegments: [] });
    expect(screen.getByText('No audio found')).toBeTruthy();
  });

  it('renders clips when segments are provided', () => {
    const { container } = renderDisplay({ audioSegments: [seg('1', '09:00')] });
    expect(screen.queryByText('No audio found')).toBeNull();
    const paper = container.querySelector('.MuiPaper-root');
    expect(paper?.childNodes.length).toBeGreaterThan(0);
  });

  it('renders a warning icon for a segment with evaluation decisions', () => {
    renderDisplay({
      audioSegments: [
        makeMockAudioSegment(
          '1',
          new Date('2026-04-20T09:00:00Z').toISOString(),
          new Date('2026-04-20T09:00:05Z').toISOString(),
          'audio1.m4a',
          ['rule1']
        ),
      ],
    });
    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('renders the waveform from peaks cached under the getAudioUrl-transformed playbackAudioUri', () => {
    const segment = makeMockAudioSegment(
      '1',
      new Date('2026-04-20T09:00:00Z').toISOString(),
      new Date('2026-04-20T09:00:05Z').toISOString(),
      'gs://bucket/audio1.m4a'
    );
    // The player only appears when peaks are found under the getAudioUrl key, so
    // its presence (with peaks) proves the playbackAudioUri transform is the key.
    __primePeaksCacheForTest(getAudioUrl(segment.playbackAudioUri ?? ''));
    renderDisplay({ audioSegments: [segment] });

    const wavesurfer = screen.getByTestId('wavesurfer-player');
    expect(wavesurfer.getAttribute('data-peaks-len')).toBe('3');
  });

  it('renders the play button and calls onTogglePlayPause when clicked', () => {
    const onTogglePlayPause = vi.fn();
    renderDisplay({ audioSegments: [seg('1', '09:00')], onTogglePlayPause });
    fireEvent.click(screen.getByLabelText('play'));
    expect(onTogglePlayPause).toHaveBeenCalled();
  });

  it('disables the play button when there are no segments', () => {
    const onTogglePlayPause = vi.fn();
    renderDisplay({ audioSegments: [], onTogglePlayPause });
    const playButton = screen.getByLabelText('play');
    expect(playButton).toBeDisabled();
    fireEvent.click(playButton);
    expect(onTogglePlayPause).not.toHaveBeenCalled();
  });

  it('renders the pause button while playing', () => {
    renderDisplay({ isAudioPlaying: true });
    expect(screen.getByLabelText('pause')).toBeTruthy();
  });

  it('seeks the playing clip cursor when currentTimeSeconds is provided', () => {
    // The cursor lives on the waveform, which only renders once peaks are cached.
    __primePeaksCacheForTest(getAudioUrl('1.m4a'));
    renderDisplay({
      audioSegments: [seg('1', '09:00')],
      currentlyPlayingSegmentId: '1',
      isAudioPlaying: true,
      currentTimeSeconds: 3.5,
    });
    expect(mockSetTime).toHaveBeenCalledWith(3.5);
  });

  it('polls currentAudioRef to seek the playing clip cursor', async () => {
    __primePeaksCacheForTest(getAudioUrl('1.m4a'));
    const mockHowl = { seek: vi.fn().mockReturnValue(2.5) };
    renderDisplay({
      audioSegments: [seg('1', '09:00')],
      currentlyPlayingSegmentId: '1',
      isAudioPlaying: true,
      currentAudioRef: { current: mockHowl as unknown as Howl },
    });

    await waitFor(() => {
      expect(mockHowl.seek).toHaveBeenCalled();
      expect(mockSetTime).toHaveBeenCalledWith(2.5);
    });
  });
});
