// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen, waitFor } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import type { PlaybackController } from '../../audio/WebAudioPlayer';
import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';
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
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('should shift window when playing segment is outside window', async () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
      makeMockAudioSegment(
        '2',
        'feed1',
        new Date('2026-04-20T08:20:00Z').toISOString(),
        new Date('2026-04-20T08:20:05Z').toISOString(),
        'Test 2',
        'audio2.m4a'
      ),
    ];

    const { rerender } = render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId="2"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
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

  it('follows the playhead into a non-speech bundle child that is off-window', async () => {
    // A long bundle (40 min) whose id is not the child ids playback advances to.
    const bundle = {
      ...makeMockAudioSegment(
        'ns-bundle',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:40:00Z').toISOString(),
        '',
        'audio.m4a'
      ),
      isNonSpeechBundle: true,
      bundledSegmentIds: ['c-early', 'c-late'],
    };
    const rawSegments: AudioSegment[] = [
      makeMockAudioSegment(
        'c-early',
        'feed1',
        new Date('2026-04-20T09:01:00Z').toISOString(),
        new Date('2026-04-20T09:01:05Z').toISOString(),
        '',
        'a.m4a'
      ),
      makeMockAudioSegment(
        'c-late',
        'feed1',
        new Date('2026-04-20T09:39:00Z').toISOString(),
        new Date('2026-04-20T09:39:05Z').toISOString(),
        '',
        'b.m4a'
      ),
    ];

    const { rerender } = render(
      <AudioDisplay
        audioSegments={[bundle]}
        rawAudioSegments={rawSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId={null}
      />
    );
    const before = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    // Playing an early child sits far outside the live-edge window; the window
    // should follow it (it can't if the child id isn't resolved to the bundle).
    rerender(
      <AudioDisplay
        audioSegments={[bundle]}
        rawAudioSegments={rawSegments}
        currentlyPlayingSegmentId="c-early"
        onClipClick={vi.fn()}
        isAudioPlaying
        playbackState="playing"
        highlightedSegmentId={null}
      />
    );

    await waitFor(() => {
      const after = screen
        .getAllByText(/\d{2}:\d{2}/)
        .map((el) => el.textContent);
      expect(after).not.toEqual(before);
    });
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

  it('should adjust window duration based on userDuration capped at 15 minutes', async () => {
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

    const { rerender } = render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        userDuration="5"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId={null}
      />
    );

    const labels5 = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent || '');
    expect(labels5.length).toBe(4);
    const [h0, m0] = labels5[0].split(':').map(Number);
    const [h3, m3] = labels5[3].split(':').map(Number);
    let diff5 = h3 * 60 + m3 - (h0 * 60 + m0);
    if (diff5 < 0) diff5 += 24 * 60;
    expect(diff5).toBe(10);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        userDuration="30"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId={null}
      />
    );

    await waitFor(() => {
      const labels30 = screen
        .getAllByText(/\d{2}:\d{2}/)
        .map((el) => el.textContent || '');
      expect(labels30.length).toBe(4);
      const [h0_30, m0_30] = labels30[0].split(':').map(Number);
      const [h3_30, m3_30] = labels30[3].split(':').map(Number);
      let diff = h3_30 * 60 + m3_30 - (h0_30 * 60 + m0_30);
      if (diff < 0) diff += 24 * 60;
      expect(diff).toBe(MAX_WINDOW_DURATION_MS / 60 / 1000);
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
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByTestId('waveform-missing-block')).toBeTruthy();
    expect(screen.queryByTestId('waveform-silence-line')).toBeNull();
  });

  it('should shift window when highlighted segment is outside window', async () => {
    const mockAudioSegments: AudioSegment[] = [
      makeMockAudioSegment(
        '1',
        'feed1',
        new Date('2026-04-20T09:00:00Z').toISOString(),
        new Date('2026-04-20T09:00:05Z').toISOString(),
        'Test 1',
        'audio1.m4a'
      ),
      makeMockAudioSegment(
        '2',
        'feed1',
        new Date('2026-04-20T08:20:00Z').toISOString(),
        new Date('2026-04-20T08:20:05Z').toISOString(),
        'Test 2',
        'audio2.m4a'
      ),
    ];

    const { rerender } = render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        rawAudioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        playbackState="listening"
        highlightedSegmentId="2"
      />
    );

    await waitFor(() => {
      const labelsAfter = screen
        .getAllByText(/\d{2}:\d{2}/)
        .map((el) => el.textContent);
      expect(labelsAfter).not.toEqual(labelsBefore);
    });
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
