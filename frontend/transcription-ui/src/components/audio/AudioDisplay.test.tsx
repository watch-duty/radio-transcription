// @vitest-environment jsdom
import React from 'react';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen, waitFor } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import type { PlaybackController } from '../../audio/WebAudioPlayer';
import { getAudioUrl } from '../../utils/audioUtils';
import { AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { AudioDisplay } from './AudioDisplay';

const mockSetTime = vi.fn();
const mockSetOptions = vi.fn();

vi.mock('@wavesurfer/react', () => {
  const MockWavesurferPlayer = (props: {
    url: string;
    onReady?: (ws: {
      setTime: (time: number) => void;
      setOptions: (opts: unknown) => void;
    }) => void;
    onDestroy?: () => void;
  }) => {
    const { onReady, onDestroy } = props;
    React.useEffect(() => {
      if (onReady) {
        onReady({
          setTime: mockSetTime,
          setOptions: mockSetOptions,
        });
      }
      return () => {
        if (onDestroy) {
          onDestroy();
        }
      };
    }, [onReady, onDestroy]);

    return <div data-testid="wavesurfer-player" data-url={props.url} />;
  };

  return {
    default: MockWavesurferPlayer,
  };
});

function makeMockAudioSegment(
  id: string,
  feedId: string,
  startTimestamp: string,
  endTimestamp: string,
  transcript: string,
  playbackAudioUri: string,
  evaluationDecisions: string[] = []
): AudioSegment {
  return {
    id,
    feedId,
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
        data: {
          text: transcript,
          errors: [],
        },
      },
      ...(evaluationDecisions.length > 0
        ? [
            {
              type: AnnotationType.EVALUATION,
              createdAt: startTimestamp,
              data: {
                decisions: evaluationDecisions,
                errors: [],
              },
            },
          ]
        : []),
    ],
  };
}

// AudioDisplay is a controlled view: the visible window is supplied via props
// (owned by useAudioTimelineWindow). Window follow/recenter behavior is covered
// by useAudioTimelineWindow.test.ts.
type DisplayProps = React.ComponentProps<typeof AudioDisplay>;
function renderDisplay(overrides: Partial<DisplayProps> = {}) {
  const props: DisplayProps = {
    audioSegments: [],
    currentlyPlayingSegmentId: null,
    highlightedSegmentId: null,
    onClipClick: vi.fn(),
    windowEndTime: null,
    windowDurationMs: AUDIO_WINDOW_DURATION_MS,
    isAudioPlaying: false,
    ...overrides,
  };
  return render(<AudioDisplay {...props} />);
}

describe('AudioDisplay', () => {
  afterEach(() => {
    cleanup();
    mockSetTime.mockClear();
    mockSetOptions.mockClear();
  });

  it('should render empty state when no transcripts', () => {
    renderDisplay({ audioSegments: [] });
    expect(screen.getByText('No audio found')).toBeTruthy();
  });

  it('should render transcripts when provided', () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
    ];

    const { container } = renderDisplay({ audioSegments: mockAudioSegments });

    expect(screen.queryByText('No audio found')).toBeNull();

    const paper = container.querySelector('.MuiPaper-root');
    expect(paper).toBeTruthy();
    expect(paper?.childNodes.length).toBeGreaterThan(0);
  });

  it('should render warning icon when transcript has evaluation decisions', () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a',
        ['rule1']
      ),
    ];

    renderDisplay({ audioSegments: mockAudioSegments });

    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('renders time-axis labels spanning the provided window duration', () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:15:00Z').toISOString(),
        new Date('2026-04-20T09:15:00Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
    ];

    renderDisplay({
      audioSegments: mockAudioSegments,
      windowEndTime: new Date('2026-04-20T09:15:00Z').getTime(),
      windowDurationMs: AUDIO_WINDOW_DURATION_MS,
    });

    const labels = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent || '');
    expect(labels.length).toBe(4);
    const [h0, m0] = labels[0].split(':').map(Number);
    const [h3, m3] = labels[3].split(':').map(Number);
    let diff = h3 * 60 + m3 - (h0 * 60 + m0);
    if (diff < 0) diff += 24 * 60;
    expect(diff).toBe(AUDIO_WINDOW_DURATION_MS / 60 / 1000);
  });

  it('passes playbackAudioUri to WavesurferPlayer (transformed via getAudioUrl)', () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'gs://bucket/audio1.m4a'
      ),
    ];

    renderDisplay({ audioSegments: mockAudioSegments });

    const wavesurfer = screen.getByTestId('wavesurfer-player');
    expect(wavesurfer).toBeTruthy();
    expect(wavesurfer.getAttribute('data-url')).toBe(
      getAudioUrl(mockAudioSegments[0].playbackAudioUri ?? '')
    );
    expect(wavesurfer.getAttribute('data-url')).toContain('.m4a');
  });

  it('should poll progress from currentAudioRef and seek wavesurfer player', async () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
    ];

    const mockPlayer = {
      getCurrentTime: vi.fn().mockReturnValue(2.5),
    };

    const currentAudioRef = {
      current: mockPlayer as unknown as PlaybackController,
    };

    renderDisplay({
      audioSegments: mockAudioSegments,
      currentlyPlayingSegmentId: '1',
      isAudioPlaying: true,
      currentAudioRef,
    });

    await waitFor(() => {
      expect(mockPlayer.getCurrentTime).toHaveBeenCalled();
    });

    expect(mockSetTime).toHaveBeenCalledWith(2.5);
  });
});
