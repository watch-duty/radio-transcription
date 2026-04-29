// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

describe('TranscriptActionsBar', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockSetRefreshInterval = vi.fn();
  const mockOnRefresh = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal('navigator', {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  afterEach(() => {
    cleanup();
  });

  it('shows source url link when sourceUrl is available', () => {
    render(
      <TranscriptActionsBar
        feedId="test-feed"
        sourceUrl="https://test.example/source"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
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
        feedId="test-feed"
        archiveUrl="https://test.example/archives"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
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
        feedId="test-feed"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('displays the manual refresh option conditionally', () => {
    const mockRefresh = vi.fn().mockResolvedValue(undefined);

    render(
      <TranscriptActionsBar
        feedId="test-feed"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockRefresh}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    const refreshButton = screen.getByRole('button', {
      name: 'refresh',
    });
    expect(refreshButton).toBeTruthy();

    fireEvent.click(refreshButton);
    expect(mockRefresh).toHaveBeenCalledTimes(1);
  });

  it('hides manual refresh button when newer transcripts exist', () => {
    render(
      <TranscriptActionsBar
        feedId="test-feed"
        hasNewerTranscripts={true}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    expect(screen.queryByRole('button', { name: 'refresh' })).toBeNull();
  });

  it('displays refresh interval options and handles change', () => {
    render(
      <TranscriptActionsBar
        feedId="test-feed"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    const intervalButton = screen.getByLabelText('select refresh interval');
    expect(intervalButton.textContent).toBe('10s');

    fireEvent.click(intervalButton);

    const option5s = screen.getByText('5s');
    expect(option5s).toBeTruthy();
    fireEvent.click(option5s);

    expect(mockSetRefreshInterval).toHaveBeenCalledWith(5000);
  });

  it('handles Share feed link click event', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(
      <TranscriptActionsBar
        feedId="test-feed"
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        triggerSnackbar={mockTriggerSnackbar}
      />
    );

    const shareButton = screen.getByRole('button', {
      name: 'copy feed deeplink',
    });
    expect(shareButton).toBeTruthy();

    fireEvent.click(shareButton);

    expect(writeTextMock).toHaveBeenCalledTimes(1);
    expect(writeTextMock).toHaveBeenCalledWith(
      expect.stringContaining('feedId=test-feed')
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Link copied');
  });
});
