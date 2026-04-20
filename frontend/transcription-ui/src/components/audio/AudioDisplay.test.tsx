// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { AudioDisplay } from './AudioDisplay';

vi.mock('@wavesurfer/react', () => ({
  default: () => <div data-testid="wavesurfer-player" />,
}));
import type { Transcript } from '@transcription/common';

describe('AudioDisplay', () => {
  afterEach(() => {
    cleanup();
  });

  it('should render empty state when no transcripts', () => {
    render(<AudioDisplay transcripts={[]} currentlyPlayingTransmissionId={null} />);
    expect(screen.getByText('No transcripts loaded')).toBeTruthy();
  });



  it('should render transcripts when provided', () => {
    const mockTranscripts: Transcript[] = [
      {
        transmissionId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        evaluationDecisions: [],
      },
    ];

    const { container } = render(
      <AudioDisplay transcripts={mockTranscripts} currentlyPlayingTransmissionId={null} />
    );
    
    expect(screen.queryByText('No transcripts loaded')).toBeNull();
    
    const paper = container.querySelector('.MuiPaper-root');
    expect(paper).toBeTruthy();
    expect(paper?.childNodes.length).toBeGreaterThan(0);
  });

  it('should render warning icon when transcript has evaluation decisions', () => {
    const mockTranscripts: Transcript[] = [
      {
        transmissionId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        evaluationDecisions: ['rule1'],
      },
    ];

    render(
      <AudioDisplay transcripts={mockTranscripts} currentlyPlayingTransmissionId={null} />
    );
    
    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('should shift window when playing transmission is outside window', async () => {
    const mockTranscripts: Transcript[] = [
      {
        transmissionId: '1',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T09:00:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T09:00:05Z').toISOString(),
        transcript: 'Test 1',
        canonicalAudioUri: 'audio1.flac',
        evaluationDecisions: [],
      },
      {
        transmissionId: '2',
        feedId: 'feed1',
        startTimestamp: new Date('2026-04-20T08:40:00Z').toISOString(),
        endTimestamp: new Date('2026-04-20T08:40:05Z').toISOString(),
        transcript: 'Test 2',
        canonicalAudioUri: 'audio2.flac',
        evaluationDecisions: [],
      },
    ];

    const { rerender } = render(
      <AudioDisplay transcripts={mockTranscripts} currentlyPlayingTransmissionId={null} />
    );

    const labelsBefore = screen.getAllByText(/\d{2}:\d{2}/).map(el => el.textContent);

    rerender(
      <AudioDisplay transcripts={mockTranscripts} currentlyPlayingTransmissionId="2" />
    );

    await waitFor(() => {
      const labelsAfter = screen.getAllByText(/\d{2}:\d{2}/).map(el => el.textContent);
      expect(labelsAfter).not.toEqual(labelsBefore);
    });
  });
});
