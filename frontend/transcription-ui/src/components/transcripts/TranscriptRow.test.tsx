// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import type { Transcript } from '@transcription/common';

import TranscriptRow from './TranscriptRow';

const mockTranscript = {
  transmissionId: 'tx-123',
  feedId: 'feed-123',
  startTimestamp: '2026-04-15T16:00:00Z',
  endTimestamp: '2026-04-15T16:00:05Z',
  canonicalAudioUri: 'https://watchduty.example/audio.mp3',
  transcript: 'This is a test transcription',
  evaluationDecisions: ['rule-1'],
} as unknown as Transcript;

describe('TranscriptRow', () => {
  const mockOnPlay = vi.fn();
  const mockTriggerSnackbar = vi.fn();
  const ruleIdToNameMap = new Map([['rule-1', 'Danger Rule']]);

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

  it('renders transcript detail accurately without Day Header when showHeader is false', () => {
    render(
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
          ruleIdToNameMap={ruleIdToNameMap}
          rulesLoading={false}
          onPlay={mockOnPlay}
          currentlyPlayingTransmissionId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={false}
        />
      </MemoryRouter>
    );

    expect(screen.getByText('This is a test transcription')).toBeTruthy();
    // The date should NOT be rendered
    expect(screen.queryByText(/Monday/i)).toBeNull();
  });

  it('renders Day Header accurately when showHeader is true', () => {
    render(
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
          ruleIdToNameMap={ruleIdToNameMap}
          rulesLoading={false}
          onPlay={mockOnPlay}
          currentlyPlayingTransmissionId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={true}
        />
      </MemoryRouter>
    );

    expect(screen.getByText(/Wednesday/i)).toBeTruthy();
  });

  it('triggers copy transcript clipboard action successfully', () => {
    render(
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
          ruleIdToNameMap={ruleIdToNameMap}
          rulesLoading={false}
          onPlay={mockOnPlay}
          currentlyPlayingTransmissionId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={false}
        />
      </MemoryRouter>
    );

    const copyButton = screen.getAllByLabelText('copy transcript')[0];
    fireEvent.click(copyButton);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'This is a test transcription'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript copied');
  });

  it('triggers copy deeplink action successfully', () => {
    render(
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
          ruleIdToNameMap={ruleIdToNameMap}
          rulesLoading={false}
          onPlay={mockOnPlay}
          currentlyPlayingTransmissionId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={false}
        />
      </MemoryRouter>
    );

    const deepLinkButton = screen.getAllByLabelText('copy deeplink')[0];
    fireEvent.click(deepLinkButton);

    const expectedStartTimestamp = new Date(mockTranscript.startTimestamp).getTime() - 300000;
    const expectedEndTimestamp = new Date(mockTranscript.endTimestamp).getTime() + 300000;

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining('feedId=feed-123')
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining('transmissionId=tx-123')
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining(`startTimestamp=${expectedStartTimestamp}`)
    );
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      expect.stringContaining(`endTimestamp=${expectedEndTimestamp}`)
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Link copied');
  });
});
