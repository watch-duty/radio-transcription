// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

describe('TranscriptActionsBar', () => {
  const mockSetRefreshInterval = vi.fn();
  const mockOnRefresh = vi.fn();
  const mockSetRedactTranscripts = vi.fn();

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

  it('displays the manual refresh option conditionally', () => {
    const mockRefresh = vi.fn().mockResolvedValue(undefined);

    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockRefresh}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
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
        searchedTimestamp={null}
        hasNewerTranscripts={true}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
      />
    );

    expect(screen.queryByRole('button', { name: 'refresh' })).toBeNull();
  });

  it('displays refresh interval options and handles change', () => {
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
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

  it('renders the redact switch and toggles state when changed', () => {
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        isTranscriptsFetching={false}
        isTranscriptsPolling={false}
        refreshInterval={10000}
        setRefreshInterval={mockSetRefreshInterval}
        onRefresh={mockOnRefresh}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
      />
    );

    const redactSwitch = screen.getByRole('switch', {
      name: /Redact transcripts/i,
    });
    expect(redactSwitch).toBeTruthy();
    expect((redactSwitch as HTMLInputElement).checked).toBe(false);

    fireEvent.click(redactSwitch);
    expect(mockSetRedactTranscripts).toHaveBeenCalledWith(true);
  });
});
