// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import TranscriptRow from './TranscriptRow';

// Mocking TranscriptPlayControl to verify it's being called with the correct props.
// We do not need to test the actual audio player functionality here
// as that is tested separately in TranscriptPlayControl.test.tsx
vi.mock('../audio/TranscriptPlayControl', () => ({
  default: (props: { audioUri: string; segmentId: string }) => (
    <div
      data-testid={`audio-player-${props.segmentId}`}
      data-audio-uri={props.audioUri}
    >
      TranscriptPlayControl Mock
    </div>
  ),
}));

let mockIsAdmin = false;
const mockSaveAs = vi.fn();

vi.mock('file-saver', () => ({
  saveAs: (...args: unknown[]) => mockSaveAs(...args),
}));

vi.mock('../../context/AuthContext', () => ({
  useAuth: vi.fn(() => ({
    isAdmin: mockIsAdmin,
  })),
}));

const mockAudioSegment: AudioSegment = {
  id: 'tx-123',
  feedId: 'feed-123',
  classification: AudioClassification.SPEECH,
  startTimestamp: '2026-04-15T16:00:00Z',
  endTimestamp: '2026-04-15T16:00:05Z',
  canonicalAudioUri: 'https://watchduty.example/audio.flac',
  playbackAudioUri: 'https://watchduty.example/audio.m4a',
  missingPriorContext: false,
  missingPostContext: false,
  sourceAudioUris: ['https://watchduty.example/audio.flac'],
  startAudioOffset: '0',
  endAudioOffset: '0',
  createdAt: '2026-04-15T16:00:00Z',
  annotations: [
    {
      type: AnnotationType.TRANSCRIPT,
      createdAt: '2026-04-15T16:00:00Z',
      data: {
        text: 'This is a test transcription',
        errors: [],
      },
    },
    {
      type: AnnotationType.EVALUATION,
      createdAt: '2026-04-15T16:00:00Z',
      data: {
        decisions: ['rule-1'],
        errors: [],
      },
    },
  ],
};

describe('TranscriptRow', () => {
  const mockOnToggleAudio = vi.fn();
  const mockOnRowClick = vi.fn();
  const mockTriggerSnackbar = vi.fn();
  const ruleIdToNameMap = new Map([['rule-1', 'Danger Rule']]);
  const queryClient = new QueryClient();

  beforeEach(() => {
    vi.clearAllMocks();
    mockIsAdmin = false;
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockImplementation(() => Promise.resolve()),
      },
    });
  });

  afterEach(() => {
    cleanup();
  });

  it('renders transcript detail accurately without Day Header when showHeader is false', () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    expect(screen.getByText('This is a test transcription')).toBeTruthy();
    expect(screen.getByText('5 sec')).toBeTruthy();
    // The date should NOT be rendered
    expect(screen.queryByText(/Monday/i)).toBeNull();
  });

  it('highlights matched spans from the evaluation annotation', () => {
    const highlightedSegment: AudioSegment = {
      ...mockAudioSegment,
      annotations: [
        {
          type: AnnotationType.TRANSCRIPT,
          createdAt: '2026-04-15T16:00:00Z',
          data: { text: 'This is a test transcription', errors: [] },
        },
        {
          type: AnnotationType.EVALUATION,
          createdAt: '2026-04-15T16:00:00Z',
          data: {
            decisions: ['rule-1'],
            errors: [],
            ruleAnnotations: {
              'rule-1': {
                textMatch: [
                  { startIndex: 10, endIndex: 14, matchedText: 'test' },
                ],
              },
            },
          },
        },
      ],
    };

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={highlightedSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    // The matched substring renders in its own highlighted element.
    expect(screen.getByText('test')).toBeTruthy();
  });

  it('renders Day Header accurately when showHeader is true', () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={true}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    expect(screen.getByText(/Wednesday/i)).toBeTruthy();
  });

  it('triggers copy transcript clipboard action successfully', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByLabelText('Share'));
    const copyButton = await screen.findByRole('menuitem', {
      name: /copy transcript/i,
    });
    fireEvent.click(copyButton);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'This is a test transcription'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript copied');
  });

  it('triggers copy deeplink action successfully', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByLabelText('Share'));
    const deepLinkButton = await screen.findByRole('menuitem', {
      name: /copy link/i,
    });
    fireEvent.click(deepLinkButton);

    const startMs = new Date(mockAudioSegment.startTimestamp).getTime();

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining('feedId=feed-123')
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining('segmentId=tx-123')
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining(`timestamp=${startMs}`)
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript link copied');
  });

  it('passes playbackAudioUri to AudioPlayer as audioUri prop', () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    const audioPlayer = screen.getByTestId(
      `audio-player-${mockAudioSegment.id}`
    );
    expect(audioPlayer).toBeTruthy();
    expect(audioPlayer.getAttribute('data-audio-uri')).toBe(
      mockAudioSegment.playbackAudioUri
    );
  });

  it('triggers download audio action successfully when download button is clicked', async () => {
    const mockBlob = new Blob(['mock audio data'], { type: 'audio/m4a' });
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok: true,
      blob: () => Promise.resolve(mockBlob),
    } as Response);

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={{
              ...mockAudioSegment,
              playbackAudioUri:
                'gs://ingestion-canonical-bucket-dev/playback/test-audio.m4a',
            }}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByLabelText('Share'));
    const downloadButton = await screen.findByRole('menuitem', {
      name: /download audio/i,
    });
    fireEvent.click(downloadButton);

    await vi.waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        '/gcs/ingestion-canonical-bucket-dev/playback/test-audio.m4a'
      );
      expect(mockSaveAs).toHaveBeenCalledWith(mockBlob, 'test-audio.m4a');
      expect(mockTriggerSnackbar).toHaveBeenCalledWith('Audio downloaded');
    });

    fetchSpy.mockRestore();
  });

  it('blurs the transcript but keeps physical text selection and copy transcript capabilities when redactTranscripts is true', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
            redactTranscripts={true}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    const transcriptText = screen.getByText('This is a test transcription');
    expect(transcriptText).toBeTruthy();
    const styles = window.getComputedStyle(transcriptText);
    expect(styles.filter).toBe('blur(6px)');
    expect(styles.opacity).toBe('0.6');
    expect(styles.userSelect).not.toBe('none');

    fireEvent.click(screen.getByLabelText('Share'));
    const copyButton = await screen.findByRole('menuitem', {
      name: /copy transcript/i,
    });
    expect(copyButton).toBeTruthy();
    expect(
      (copyButton as HTMLElement).getAttribute('aria-disabled')
    ).toBeNull();

    fireEvent.click(copyButton);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'This is a test transcription'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript copied');
  });

  it('renders silence bundle correctly with placeholder text and disabled copy', async () => {
    const mockSilenceBundle: RenderableAudioSegment = {
      id: 'silence-123',
      feedId: 'feed-123',
      classification: AudioClassification.OTHER,
      startTimestamp: '2026-04-15T16:00:00Z',
      endTimestamp: '2026-04-15T16:00:10Z',
      playbackAudioUri: 'https://watchduty.example/silence.m4a',
      isSilenceBundle: true,
      bundledSegmentIds: ['silence-123', 'silence-124'],
      createdAt: '2026-04-15T16:00:00Z',
      annotations: [],
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
    };

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockSilenceBundle}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    expect(screen.getByText('[No speech detected]')).toBeTruthy();
    expect(screen.getByText('10 sec')).toBeTruthy();

    fireEvent.click(screen.getByLabelText('Share'));
    await screen.findByRole('menuitem', { name: /copy link/i });
    expect(
      screen.queryByRole('menuitem', { name: /copy transcript/i })
    ).toBeNull();
  });

  it('shows coarse elapsed time without seconds when silence row is at the live edge (ongoing silence)', () => {
    const mockSilenceBundle: RenderableAudioSegment = {
      id: 'silence-123',
      feedId: 'feed-123',
      classification: AudioClassification.OTHER,
      startTimestamp: '2026-04-15T16:00:00Z',
      endTimestamp: '2026-04-15T16:00:10Z',
      playbackAudioUri: 'https://watchduty.example/silence.m4a',
      isSilenceBundle: true,
      bundledSegmentIds: ['silence-123', 'silence-124'],
      createdAt: '2026-04-15T16:00:00Z',
      annotations: [],
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
    };

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockSilenceBundle}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
            isTopAudioSegmentRow={true}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    expect(screen.getByText('[No speech detected]')).toBeTruthy();
    expect(screen.queryByText('10 sec')).toBeNull();
    expect(screen.getByText('<1 min')).toBeTruthy();
  });

  it('does not render segment details in the share popover for non-admins', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByLabelText('Share'));
    await screen.findByRole('menuitem', { name: /copy link/i });
    expect(screen.queryByText('Segment ID')).not.toBeInTheDocument();
  });

  it('renders segment details in the share popover for admins', async () => {
    mockIsAdmin = true;

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockAudioSegment}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByLabelText('Segment info'));
    expect(await screen.findByText('Segment ID')).toBeInTheDocument();
  });

  it('renders transcription failure correctly with placeholder text and disabled copy', async () => {
    const mockFailedTranscript: AudioSegment = {
      ...mockAudioSegment,
      annotations: [
        {
          type: AnnotationType.TRANSCRIPT,
          createdAt: '2026-04-15T16:00:00Z',
          data: {
            text: '',
            errors: ['some API error'],
          },
        },
      ],
    };

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockFailedTranscript}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    expect(screen.getByText('[Transcription failed]')).toBeTruthy();

    fireEvent.click(screen.getByLabelText('Share'));
    const copyButton = await screen.findByRole('menuitem', {
      name: /copy transcript/i,
    });
    expect((copyButton as HTMLElement).getAttribute('aria-disabled')).toBe(
      'true'
    );
  });

  it('renders partial transcription with text and incomplete prefix, and copy button enabled', async () => {
    const mockPartialTranscript: AudioSegment = {
      ...mockAudioSegment,
      annotations: [
        {
          type: AnnotationType.TRANSCRIPT,
          createdAt: '2026-04-15T16:00:00Z',
          data: {
            text: 'This is a partial transcript text',
            errors: ['Partial transcription (max_tokens)'],
          },
        },
      ],
    };

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <TranscriptRow
            audioSegment={mockPartialTranscript}
            index={0}
            totalAudioSegments={1}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={false}
            onToggleAudio={mockOnToggleAudio}
            isAudioPlaying={false}
            onRowClick={mockOnRowClick}
            currentlyPlayingSegmentId={null}
            triggerSnackbar={mockTriggerSnackbar}
            showHeader={false}
          />
        </MemoryRouter>
      </QueryClientProvider>
    );

    // Should render the text itself, NOT [Transcription failed]
    expect(screen.getByText('This is a partial transcript text')).toBeTruthy();
    expect(screen.getByText('[Transcript may be incomplete]')).toBeTruthy();
    expect(screen.queryByText('[Transcription failed]')).toBeNull();
    // Copy button should be enabled
    fireEvent.click(screen.getByLabelText('Share'));
    const copyButton = await screen.findByRole('menuitem', {
      name: /copy transcript/i,
    });
    expect(
      (copyButton as HTMLElement).getAttribute('aria-disabled')
    ).toBeNull();
  });
});
