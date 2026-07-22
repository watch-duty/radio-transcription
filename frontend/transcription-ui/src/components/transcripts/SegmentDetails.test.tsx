// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { SegmentDetails } from './SegmentDetails';

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

describe('SegmentDetails', () => {
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

  it('renders segment and external IDs and copies them', () => {
    const segmentWithExtId = {
      ...mockAudioSegment,
      externalAudioSegmentId: 'ext-segment-abc-123',
    };

    render(
      <SegmentDetails
        audioSegment={segmentWithExtId}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    expect(screen.getByText('Segment ID')).toBeInTheDocument();
    expect(screen.getByText('tx-123')).toBeInTheDocument();
    expect(screen.getByText('ext-segment-abc-123')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('copy segment id'));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('tx-123');
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Segment ID copied');

    fireEvent.click(screen.getByLabelText('copy external segment id'));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'ext-segment-abc-123'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith(
      'External segment ID copied'
    );
  });

  it('does not render external segment ID when not present', () => {
    render(
      <SegmentDetails
        audioSegment={mockAudioSegment}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    expect(screen.getByText('tx-123')).toBeInTheDocument();
    expect(
      screen.queryByLabelText('copy external segment id')
    ).not.toBeInTheDocument();
  });

  it('renders degradation reasons as segment errors', () => {
    render(
      <SegmentDetails
        audioSegment={mockAudioSegment}
        triggerSnackbar={mockTriggerSnackbar}
        degradationReasons={[
          'Audio cut off at the end',
          'System max tokens reached',
        ]}
      />
    );

    expect(screen.getByText('Segment error(s)')).toBeInTheDocument();
    expect(screen.getByText('Audio cut off at the end')).toBeInTheDocument();
    expect(screen.getByText('System max tokens reached')).toBeInTheDocument();
  });
});
