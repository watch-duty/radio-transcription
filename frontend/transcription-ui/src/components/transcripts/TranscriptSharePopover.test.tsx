// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { TranscriptSharePopover } from './TranscriptSharePopover';

vi.mock('../../context/AuthContext', () => ({
  useAuth: vi.fn(() => ({ isAdmin: true })),
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
  annotations: [],
};

function renderPopover(
  overrides: Partial<AudioSegment> = {},
  reasons?: string[]
) {
  return render(
    <TranscriptSharePopover
      audioSegment={{ ...mockAudioSegment, ...overrides }}
      transcriptAnnotation={null}
      isSilence={false}
      isOutage={false}
      hasErrors={false}
      degradationReasons={reasons ?? []}
      triggerSnackbar={mockTriggerSnackbar}
    />
  );
}

const mockTriggerSnackbar = vi.fn();
const openPopover = () => fireEvent.click(screen.getByLabelText('Share'));
const openInfoPopover = () =>
  fireEvent.click(screen.getByLabelText('Segment info'));

describe('TranscriptSharePopover', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockImplementation(() => Promise.resolve()),
      },
    });
  });

  afterEach(() => {
    cleanup();
  });

  it('copies the transcript link from the popover', async () => {
    renderPopover();
    openPopover();

    fireEvent.click(
      await screen.findByRole('menuitem', { name: /copy link/i })
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining('segmentId=tx-123')
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript link copied');
  });

  it('shows segment and external IDs and copies them for admins', async () => {
    renderPopover({ externalAudioSegmentId: 'ext-segment-abc-123' });
    openInfoPopover();

    expect(await screen.findByText('Segment ID')).toBeInTheDocument();
    expect(screen.getByText('tx-123')).toBeInTheDocument();
    expect(screen.getByText('ext-segment-abc-123')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('copy segment id'));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('tx-123');
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Segment ID copied');

    fireEvent.click(screen.getByLabelText('copy external segment id'));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'ext-segment-abc-123'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('External ID copied');
  });

  it('omits the external ID when not present', async () => {
    renderPopover();
    openInfoPopover();

    expect(await screen.findByText('Segment ID')).toBeInTheDocument();
    expect(
      screen.queryByLabelText('copy external segment id')
    ).not.toBeInTheDocument();
  });

  it('renders degradation reasons as segment errors', async () => {
    renderPopover({}, [
      'Audio cut off at the end',
      'System max tokens reached',
    ]);
    openInfoPopover();

    expect(await screen.findByText('Segment error(s)')).toBeInTheDocument();
    expect(screen.getByText('Audio cut off at the end')).toBeInTheDocument();
    expect(screen.getByText('System max tokens reached')).toBeInTheDocument();
  });
});
