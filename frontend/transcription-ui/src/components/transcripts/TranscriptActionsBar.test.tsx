// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

describe('TranscriptActionsBar', () => {
  afterEach(() => {
    cleanup();
  });

  it('shows source url link when sourceUrl is available', () => {
    render(
      <TranscriptActionsBar
        sourceUrl="https://test.example/source"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        pollingIntervalDisplay="15s"
        onRefresh={vi.fn()}
      />
    );

    const link = screen.getByText(/original source link/i);
    expect(link).toBeTruthy();
    expect(link.getAttribute('href')).toBe('https://test.example/source');
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
  });

  it('shows archive url link when archiveUrl is available', () => {
    render(
      <TranscriptActionsBar
        archiveUrl="https://test.example/archives"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        pollingIntervalDisplay="15s"
        onRefresh={vi.fn()}
      />
    );

    const link = screen.getByText(/archives/i);
    expect(link).toBeTruthy();
    expect(link.getAttribute('href')).toBe('https://test.example/archives');
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
  });

  it('does not render links when neither is supplied', () => {
    render(
      <TranscriptActionsBar
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        pollingIntervalDisplay="15s"
        onRefresh={vi.fn()}
      />
    );

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('displays the manual refresh option conditionally', () => {
    const mockRefresh = vi.fn().mockResolvedValue(undefined);

    render(
      <TranscriptActionsBar
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        pollingIntervalDisplay="15s"
        onRefresh={mockRefresh}
      />
    );

    const refreshButton = screen.getByRole('button', {
      name: /Refresh \(15s\)/i,
    });
    expect(refreshButton).toBeTruthy();

    fireEvent.click(refreshButton);
    expect(mockRefresh).toHaveBeenCalledTimes(1);
  });

  it('hides manual refresh button when newer transcripts exist', () => {
    render(
      <TranscriptActionsBar
        hasNewerTranscripts={true}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        pollingIntervalDisplay="15s"
        onRefresh={vi.fn()}
      />
    );

    expect(
      screen.queryByRole('button', { name: /Refresh \(15s\)/i })
    ).toBeNull();
  });

  it('displays the feed outlined status Alert and human-friendly relative time string', () => {
    const fixedNow = new Date('2026-04-10T12:05:00Z');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(fixedNow);

    try {
      render(
        <TranscriptActionsBar
          status="active"
          lastHeartbeat={new Date(
            fixedNow.getTime() - 5 * 60 * 1000
          ).toISOString()}
          hasNewerTranscripts={false}
          isTranscriptsFetching={false}
          isTranscriptsPolling={false}
          pollingIntervalDisplay="15s"
          onRefresh={vi.fn()}
        />
      );

      expect(screen.getByText('active')).toBeTruthy();
      expect(screen.getByText('Last Updated: 5 minutes ago')).toBeTruthy();
    } finally {
      vi.useRealTimers();
    }
  });
});
