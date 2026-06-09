// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import type { Transcript } from '@transcription/common';

import TranscriptRow from './TranscriptRow';

// Mocking AudioPlayer to verify it's being called with the correct props.
// We do not need to test the actual audio player functionality here
// as that is tested separately in AudioPlayer.test.tsx
vi.mock('../audio/AudioPlayer', () => ({
  default: (props: { audioUri: string; segmentId: string }) => (
    <div
      data-testid={`audio-player-${props.segmentId}`}
      data-audio-uri={props.audioUri}
    >
      AudioPlayer Mock
    </div>
  ),
}));

const mockTranscript: Transcript = {
  segmentId: 'tx-123',
  feedId: 'feed-123',
  startTimestamp: '2026-04-15T16:00:00Z',
  endTimestamp: '2026-04-15T16:00:05Z',
  canonicalAudioUri: 'https://watchduty.example/audio.flac',
  playbackAudioUri: 'https://watchduty.example/audio.m4a',
  transcript: 'This is a test transcription',
  evaluationDecisions: ['rule-1'],
  missingPriorContext: false,
  missingPostContext: false,
  sourceAudioUris: ['https://watchduty.example/audio.flac'],
  startAudioOffset: '0',
  endAudioOffset: '0',
};

describe('TranscriptRow', () => {
  const mockOnToggleAudio = vi.fn();
  const mockOnRowClick = vi.fn();
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
          onToggleAudio={mockOnToggleAudio}
          isAudioPlaying={false}
          onRowClick={mockOnRowClick}
          currentlyPlayingSegmentId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={false}
        />
      </MemoryRouter>
    );

    expect(screen.getByText('This is a test transcription')).toBeTruthy();
    expect(screen.getByText('5 sec')).toBeTruthy();
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
          onToggleAudio={mockOnToggleAudio}
          isAudioPlaying={false}
          onRowClick={mockOnRowClick}
          currentlyPlayingSegmentId={null}
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
          onToggleAudio={mockOnToggleAudio}
          isAudioPlaying={false}
          onRowClick={mockOnRowClick}
          currentlyPlayingSegmentId={null}
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
          onToggleAudio={mockOnToggleAudio}
          isAudioPlaying={false}
          onRowClick={mockOnRowClick}
          currentlyPlayingSegmentId={null}
          triggerSnackbar={mockTriggerSnackbar}
          showHeader={false}
        />
      </MemoryRouter>
    );

    const deepLinkButton = screen.getAllByLabelText('copy deeplink')[0];
    fireEvent.click(deepLinkButton);

    const startMs = new Date(mockTranscript.startTimestamp).getTime();

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
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
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
    );

    const audioPlayer = screen.getByTestId(
      `audio-player-${mockTranscript.segmentId}`
    );
    expect(audioPlayer).toBeTruthy();
    expect(audioPlayer.getAttribute('data-audio-uri')).toBe(
      mockTranscript.playbackAudioUri
    );
  });

  it('blurs the transcript but keeps physical text selection and copy transcript capabilities when redactTranscripts is true', () => {
    render(
      <MemoryRouter>
        <TranscriptRow
          transcript={mockTranscript}
          index={0}
          totalTranscripts={1}
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
    );

    const transcriptText = screen.getByText('This is a test transcription');
    expect(transcriptText).toBeTruthy();
    const styles = window.getComputedStyle(transcriptText);
    expect(styles.filter).toBe('blur(6px)');
    expect(styles.opacity).toBe('0.6');
    expect(styles.userSelect).not.toBe('none');

    const copyButton = screen.getAllByLabelText('copy transcript')[0];
    expect(copyButton).toBeTruthy();
    expect((copyButton as HTMLButtonElement).disabled).toBe(false);

    fireEvent.click(copyButton);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
      'This is a test transcription'
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Transcript copied');
  });
});
