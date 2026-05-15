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
        dateTime={null}
        setDateTime={vi.fn()}
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
        dateTime={null}
        setDateTime={vi.fn()}
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
        dateTime={null}
        setDateTime={vi.fn()}
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
        dateTime={null}
        setDateTime={vi.fn()}
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

  it('opens the filter menu, clears local value, and applies new value on Apply', () => {
    const mockSetDateTime = vi.fn();
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
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const clearButton = screen.getByRole('button', { name: 'Clear' });
    fireEvent.click(clearButton);

    // Should not have applied yet
    expect(mockSetDateTime).not.toHaveBeenCalled();

    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetDateTime).toHaveBeenCalledWith(null);
  });

  it('keeps the original value if cleared and then cancelled', () => {
    const mockSetDateTime = vi.fn();
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
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const clearButton = screen.getByRole('button', { name: 'Clear' });
    fireEvent.click(clearButton);

    const cancelButton = screen.getByRole('button', { name: 'Cancel' });
    fireEvent.click(cancelButton);

    expect(mockSetDateTime).not.toHaveBeenCalled();

    // Reopen filter menu and apply to verify local state reverted to prop value
    fireEvent.click(filterButton);
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetDateTime).toHaveBeenCalledWith(new Date('2026-05-14T12:00:00Z'));
  });
});
