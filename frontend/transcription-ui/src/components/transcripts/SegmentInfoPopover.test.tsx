// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { SegmentInfoPopover } from './SegmentInfoPopover';

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

describe('SegmentInfoPopover', () => {
  const mockTriggerSnackbar = vi.fn();

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

  it('renders info button and opens popover with details on click', async () => {
    const segmentWithExtId = {
      ...mockAudioSegment,
      externalAudioSegmentId: 'ext-segment-abc-123',
    };

    render(
      <SegmentInfoPopover
        audioSegment={segmentWithExtId}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    // Verify info button is rendered
    const infoButton = screen.getByLabelText('view segment info');
    expect(infoButton).toBeInTheDocument();

    // Popover content shouldn't be visible yet
    expect(screen.queryByText('Segment Details')).not.toBeInTheDocument();

    // Click to open popover
    fireEvent.click(infoButton);

    // Verify popover is visible
    expect(await screen.findByText('Segment Details')).toBeInTheDocument();
    expect(screen.getByText('tx-123')).toBeInTheDocument();
    expect(screen.getByText('ext-segment-abc-123')).toBeInTheDocument();

    // Test copying segment ID from popover
    const copySegmentIdBtn = screen.getByLabelText('copy segment id');
    fireEvent.click(copySegmentIdBtn);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('tx-123');
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Segment ID copied');

    // Test copying external ID from popover
    const copyExternalIdBtn = screen.getByLabelText('copy external segment id');
    fireEvent.click(copyExternalIdBtn);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'ext-segment-abc-123'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith(
      'External segment ID copied'
    );
  });

  it('does not render external segment ID inside popover if not present', async () => {
    render(
      <SegmentInfoPopover
        audioSegment={mockAudioSegment}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    const infoButton = screen.getByLabelText('view segment info');
    fireEvent.click(infoButton);

    expect(await screen.findByText('Segment Details')).toBeInTheDocument();
    expect(screen.getByText('tx-123')).toBeInTheDocument();
    expect(
      screen.queryByLabelText('copy external segment id')
    ).not.toBeInTheDocument();
  });

  it('renders degradation reasons as segment errors inside the popover', async () => {
    render(
      <SegmentInfoPopover
        audioSegment={mockAudioSegment}
        triggerSnackbar={mockTriggerSnackbar}
        degradationReasons={[
          'Audio cut off at the end',
          'System max tokens reached',
        ]}
      />
    );

    const infoButton = screen.getByLabelText('view segment info');
    fireEvent.click(infoButton);

    expect(await screen.findByText('Segment error(s)')).toBeInTheDocument();
    expect(screen.getByText('Audio cut off at the end')).toBeInTheDocument();
    expect(screen.getByText('System max tokens reached')).toBeInTheDocument();
  });
});
