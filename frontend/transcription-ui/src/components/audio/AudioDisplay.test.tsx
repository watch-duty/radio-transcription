// @vitest-environment jsdom
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
import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { AudioDisplay } from './AudioDisplay';

vi.mock('@wavesurfer/react', () => ({
  default: (props: { url: string }) => (
    <div data-testid="wavesurfer-player" data-url={props.url} />
  ),
}));

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

describe('AudioDisplay', () => {
  afterEach(() => {
    cleanup();
  });

  it('should render empty state when no transcripts', () => {
    render(
      <AudioDisplay
        audioSegments={[]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        currentlyPlayingSegmentId="2"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments2}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        userDuration="5"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
        currentlyPlayingSegmentId={null}
        userDuration="30"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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

    render(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    const wavesurfer = screen.getByTestId('wavesurfer-player');
    expect(wavesurfer).toBeTruthy();
    expect(wavesurfer.getAttribute('data-url')).toBe(
      getAudioUrl(mockAudioSegments[0].playbackAudioUri ?? '')
    );
    expect(wavesurfer.getAttribute('data-url')).toContain('.m4a');
  });

  it('should render play button and call onTogglePlayPause when clicked', () => {
    const mockOnTogglePlayPause = vi.fn();
    render(
      <AudioDisplay
        audioSegments={[
          makeMockAudioSegment(
            '1',
            'feed1',
            new Date('2026-04-20T09:00:00Z').toISOString(),
            new Date('2026-04-20T09:00:05Z').toISOString(),
            'Test 1',
            'audio1.m4a'
          ),
        ]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={mockOnTogglePlayPause}
        highlightedSegmentId={null}
      />
    );

    const playButton = screen.getByLabelText('play');
    expect(playButton).toBeTruthy();
    fireEvent.click(playButton);
    expect(mockOnTogglePlayPause).toHaveBeenCalled();
  });

  it('should render disabled play button when transcripts list is empty and not call onTogglePlayPause when clicked', () => {
    const mockOnTogglePlayPause = vi.fn();
    render(
      <AudioDisplay
        audioSegments={[]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={mockOnTogglePlayPause}
        highlightedSegmentId={null}
      />
    );

    const playButton = screen.getByLabelText('play');
    expect(playButton).toBeTruthy();
    expect(playButton).toBeDisabled();
    fireEvent.click(playButton);
    expect(mockOnTogglePlayPause).not.toHaveBeenCalled();
  });

  it('should render pause button when playing', () => {
    render(
      <AudioDisplay
        audioSegments={[]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={true}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    expect(screen.getByLabelText('pause')).toBeTruthy();
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
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    const labelsBefore = screen
      .getAllByText(/\d{2}:\d{2}/)
      .map((el) => el.textContent);

    rerender(
      <AudioDisplay
        audioSegments={mockAudioSegments}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
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
});
