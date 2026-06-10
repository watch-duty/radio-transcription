// @vitest-environment jsdom
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type { Transcript } from '@transcription/common';

import { getAudioUrl } from '../../utils/audioUtils';
import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { AudioDisplay } from './AudioDisplay';
import {
  __primePeaksCacheForTest,
  __resetPeaksCacheForTest,
} from './usePeaksDecodeQueue';

vi.mock('@wavesurfer/react', () => ({
  // Display-only: the player renders from cached peaks/duration, not a url.
  default: (props: {
    peaks?: (number[] | Float32Array)[];
    duration?: number;
  }) => (
    <div
      data-testid="wavesurfer-player"
      data-peaks-len={props.peaks?.[0]?.length}
      data-duration={props.duration}
    />
  ),
}));

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

describe('AudioDisplay', () => {
  // jsdom reports 0 for layout metrics and ignores scrollLeft writes. Give the
  // scroller a viewport (1000) narrower than its content (5000) and a settable
  // scrollLeft so programmatic scrolling and the settled-viewport scan run for
  // real instead of bailing out or collapsing to a single full-range window.
  beforeAll(() => {
    Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
      configurable: true,
      get() {
        return 1000;
      },
    });
    Object.defineProperty(HTMLElement.prototype, 'scrollWidth', {
      configurable: true,
      get() {
        return 5000;
      },
    });
    Object.defineProperty(HTMLElement.prototype, 'scrollLeft', {
      configurable: true,
      get(this: { __scrollLeft?: number }) {
        return this.__scrollLeft ?? 0;
      },
      set(this: { __scrollLeft?: number }, v: number) {
        this.__scrollLeft = v;
      },
    });
  });

  afterAll(() => {
    delete (HTMLElement.prototype as { clientWidth?: number }).clientWidth;
    delete (HTMLElement.prototype as { scrollWidth?: number }).scrollWidth;
    delete (HTMLElement.prototype as { scrollLeft?: number }).scrollLeft;
  });

  afterEach(() => {
    cleanup();
    __resetPeaksCacheForTest();
  });

  it('should render empty state when no transcripts', () => {
    render(
      <AudioDisplay
        transcripts={[]}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );
    expect(screen.getByText('No transcripts loaded')).toBeTruthy();
  });

  it('should render transcripts when provided', () => {
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const { container } = render(
      <AudioDisplay
        transcripts={mockTranscripts}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    expect(screen.queryByText('No transcripts loaded')).toBeNull();

    const paper = container.querySelector('.MuiPaper-root');
    expect(paper).toBeTruthy();
    expect(paper?.childNodes.length).toBeGreaterThan(0);
  });

  it('should render warning icon when transcript has evaluation decisions', () => {
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: ['rule1'],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    render(
      <AudioDisplay
        transcripts={mockTranscripts}
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
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
      {
        segmentId: '2',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T08:20:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T08:20:05Z').toISOString(),
        transcript: 'Test 2',
        canonicalAudioUri: 'audio2.flac',
        playbackAudioUri: 'audio2.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const { rerender } = render(
      <AudioDisplay
        transcripts={mockTranscripts}
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
        transcripts={mockTranscripts}
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
    const mockTranscripts1: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const mockTranscripts2: Transcript[] = [
      {
        segmentId: '2',
        feedId: 'feed2',
        startTimestamp: new Date('2026-04-20T10:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T10:00:05Z').toISOString(),
        transcript: 'Test 2',
        canonicalAudioUri: 'audio2.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const { rerender } = render(
      <AudioDisplay
        transcripts={mockTranscripts1}
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
        transcripts={mockTranscripts2}
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
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:15:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:15:00Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const { rerender } = render(
      <AudioDisplay
        transcripts={mockTranscripts}
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
        transcripts={mockTranscripts}
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

  it('renders the waveform from peaks cached under the getAudioUrl-transformed playbackAudioUri', async () => {
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'gs://bucket/audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    // The player renders from cached peaks; seed the cache so it appears
    // synchronously (real decoding is unavailable under jsdom).
    __primePeaksCacheForTest(getAudioUrl(mockTranscripts[0].playbackAudioUri));

    render(
      <AudioDisplay
        transcripts={mockTranscripts}
        currentlyPlayingSegmentId={null}
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    // Waveforms mount lazily once the viewport settles after the initial scroll.
    // The player only appears when peaks are found under the getAudioUrl key, so
    // its presence (with peaks) proves the playbackAudioUri transform is the key.
    const wavesurfer = await screen.findByTestId('wavesurfer-player');
    expect(wavesurfer).toBeTruthy();
    expect(wavesurfer.getAttribute('data-peaks-len')).toBe('3');
    expect(wavesurfer.getAttribute('data-duration')).toBe('1');
  });

  it('should render play button and call onTogglePlayPause when clicked', () => {
    const mockOnTogglePlayPause = vi.fn();
    render(
      <AudioDisplay
        transcripts={[
          {
            segmentId: '1',
            feedId: 'feed1',
            startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
            endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
            transcript: 'Test 1',
            canonicalAudioUri: 'audio1.flac',
            playbackAudioUri: 'audio1.m4a',
            evaluationDecisions: [],
            missingPriorContext: false,
            missingPostContext: false,
            sourceAudioUris: [],
            startAudioOffset: '0',
            endAudioOffset: '0',
          },
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
        transcripts={[]}
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
        transcripts={[]}
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
    const mockTranscripts: Transcript[] = [
      {
        segmentId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        playbackAudioUri: 'audio1.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
      {
        segmentId: '2',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T08:20:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T08:20:05Z').toISOString(),
        transcript: 'Test 2',
        canonicalAudioUri: 'audio2.flac',
        playbackAudioUri: 'audio2.m4a',
        evaluationDecisions: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
        startAudioOffset: '0',
        endAudioOffset: '0',
      },
    ];

    const { rerender } = render(
      <AudioDisplay
        transcripts={mockTranscripts}
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
        transcripts={mockTranscripts}
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

  it('mounts waveforms only for clips in the settled viewport, not the whole range', async () => {
    // Range spans an hour; with a 10-min window (userDuration "5") the content is
    // ~6 viewports wide, so only the live-edge clip is in view after follow-live.
    const mk = (id: string, hhmm: string, uri: string): Transcript => ({
      segmentId: id,
      feedId: 'feed1',
      startTimestamp: new Date(`2026-04-20T${hhmm}:00Z`).toISOString(),
      endTimestamp: new Date(`2026-04-20T${hhmm}:05Z`).toISOString(),
      transcript: id,
      canonicalAudioUri: `${uri}.flac`,
      playbackAudioUri: uri,
      evaluationDecisions: [],
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
      startAudioOffset: '0',
      endAudioOffset: '0',
    });
    const mockTranscripts: Transcript[] = [
      mk('live', '10:00', 'live.m4a'),
      mk('mid', '09:30', 'mid.m4a'),
      mk('old', '09:00', 'old.m4a'),
    ];
    // Prime peaks for every clip so any mounted clip would render a waveform.
    mockTranscripts.forEach((t) =>
      __primePeaksCacheForTest(getAudioUrl(t.playbackAudioUri))
    );

    render(
      <AudioDisplay
        transcripts={mockTranscripts}
        currentlyPlayingSegmentId={null}
        userDuration="5"
        onClipClick={vi.fn()}
        isAudioPlaying={false}
        onTogglePlayPause={vi.fn()}
        highlightedSegmentId={null}
      />
    );

    // Only the live-edge clip is in the settled viewport; the off-screen clips
    // stay placeholders even though their peaks are cached.
    await waitFor(() => {
      const players = screen.getAllByTestId('wavesurfer-player');
      expect(players).toHaveLength(1);
      expect(players[0].getAttribute('data-peaks-len')).toBe('3');
    });
  });
});
