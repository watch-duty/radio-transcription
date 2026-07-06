// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen, waitFor } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import type { PlaybackController } from '../../audio/WebAudioPlayer';
import { AudioDisplay } from './AudioDisplay';

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
      {
        type: AnnotationType.WAVEFORM,
        createdAt: startTimestamp,
        data: {
          peaks: [[0.1, 0.5, 0.25]],
          durationSeconds:
            (new Date(endTimestamp).getTime() -
              new Date(startTimestamp).getTime()) /
            1000,
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

describe('AudioDisplay', () => {
  afterEach(() => {
    cleanup();
  });

  it('should render empty state when no transcripts', () => {
    render(
      <AudioDisplay
        audioSegments={[]}
        rawAudioSegments={[]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );
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

    const { container } = render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

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

    render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('should reset window when transcripts[0] changes', async () => {
    const mockAudioSegments1: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
    ];

    const mockAudioSegments2: AudioSegment[] = [
      makeMockAudioSegment(
        '2',
        'feed2',
        new Date('2026-04-20T10:00:00Z').toISOString(),
        new Date('2026-04-20T10:00:05Z').toISOString(),
        'Test 2',
        'audio1.m4a'
      ),
    ];

    const { rerender } = render(
      <AudioDisplay
        audioSegments={mockAudioSegments1}
        rawAudioSegments={mockAudioSegments1}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments2}
        rawAudioSegments={mockAudioSegments2}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    await waitFor(() => {
      const labelsAfter = screen
        .getAllByText(/\d{2}:\d{2}/)
        .map((el) => el.textContent);
      expect(labelsAfter).not.toEqual(labelsBefore);
    });
  });

  it('renders a waveform from the WAVEFORM annotation peaks', () => {
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

    render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByTestId('waveform')).toBeTruthy();
  });

  it('renders a non-silence block when a speech segment has no WAVEFORM annotation', () => {
    const segment = makeMockAudioSegment(
      '1',
      'feed1',
      new Date('2026-04-20T09:00:00Z').toISOString(),
      new Date('2026-04-20T09:00:05Z').toISOString(),
      'Test 1',
      'gs://bucket/audio1.m4a'
    );
    segment.annotations = segment.annotations.filter(
      (a) => a.type !== AnnotationType.WAVEFORM
    );

    render(
      <AudioDisplay
        audioSegments={[segment]}
        rawAudioSegments={[segment]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    expect(screen.queryByTestId('waveform')).toBeNull();
    expect(screen.getByTestId('waveform-missing-block')).toBeTruthy();
    expect(screen.queryByTestId('waveform-silence-line')).toBeNull();
  });

  it('renders a thin line when a non-speech segment has no WAVEFORM annotation', () => {
    const segment = makeMockAudioSegment(
      '1',
      'feed1',
      new Date('2026-04-20T09:00:00Z').toISOString(),
      new Date('2026-04-20T09:00:05Z').toISOString(),
      'Test 1',
      'gs://bucket/audio1.m4a'
    );
    segment.classification = AudioClassification.OTHER;
    segment.annotations = segment.annotations.filter(
      (a) =>
        a.type !== AnnotationType.WAVEFORM &&
        a.type !== AnnotationType.TRANSCRIPT
    );

    render(
      <AudioDisplay
        audioSegments={[segment]}
        rawAudioSegments={[segment]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    expect(screen.queryByTestId('waveform')).toBeNull();
    expect(screen.getByTestId('waveform-silence-line')).toBeTruthy();
    expect(screen.queryByTestId('waveform-missing-block')).toBeNull();
  });

  it('treats a transcribed UNSPECIFIED segment without a WAVEFORM annotation as speech', () => {
    const segment = makeMockAudioSegment(
      '1',
      'feed1',
      new Date('2026-04-20T09:00:00Z').toISOString(),
      new Date('2026-04-20T09:00:05Z').toISOString(),
      'Test 1',
      'gs://bucket/audio1.m4a'
    );
    segment.classification = AudioClassification.UNSPECIFIED;
    segment.annotations = segment.annotations.filter(
      (a) => a.type !== AnnotationType.WAVEFORM
    );

    render(
      <AudioDisplay
        audioSegments={[segment]}
        rawAudioSegments={[segment]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByTestId('waveform-missing-block')).toBeTruthy();
    expect(screen.queryByTestId('waveform-silence-line')).toBeNull();
  });

  it('polls progress and shows the playhead while playing', async () => {
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

    render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId="1"
        onClipClick={vi.fn()}
        isAudioPlaying={true}
        playbackState="playing"
        windowEndTime={null}
        windowDurationMs={15 * 60 * 1000}
        highlightedSegmentId={null}
        currentAudioRef={currentAudioRef}
      />
    );

    await waitFor(() => {
      expect(mockPlayer.getCurrentTime).toHaveBeenCalled();
    });

    // The label being a clock time (not "Listening") confirms the polled
    // position was wired through computePlayhead into the playhead.
    await waitFor(() => {
      expect(screen.getByTestId('timeline-playhead').textContent).toMatch(
        /^\d{2}:\d{2}:\d{2}$/
      );
    });
  });
});
