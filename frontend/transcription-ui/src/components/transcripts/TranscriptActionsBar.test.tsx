// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

// MUI's picker can't be driven through its segmented inputs under JSDOM; mock the
// shared field with buttons that write a valid / invalid draft. The popover commits.
const PICKED_DATE = new Date('2026-05-14T12:00:00.000Z');
vi.mock('../common/DateTimePicker', () => ({
  DateTimePicker: ({
    setDateTime,
  }: {
    setDateTime: (value: Date | null) => void;
  }) => (
    <>
      <button onClick={() => setDateTime(PICKED_DATE)}>set-draft-date</button>
      <button onClick={() => setDateTime(new Date('nope'))}>
        set-invalid-date
      </button>
    </>
  ),
}));

const baseProps = {
  disableJumpToLive: false,
  redactTranscripts: false,
  setRedactTranscripts: vi.fn(),
  dateTime: null as Date | null,
  setDateTime: vi.fn(),
  alertFilter: 'all' as const,
  setAlertFilter: vi.fn(),
  onClickViewLatest: vi.fn(),
};

describe('TranscriptActionsBar', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => cleanup());

  it('renders the redact switch and toggles state when changed', () => {
    const setRedactTranscripts = vi.fn();
    render(
      <TranscriptActionsBar
        {...baseProps}
        setRedactTranscripts={setRedactTranscripts}
      />
    );
    const redactSwitch = screen.getByRole('switch', {
      name: /Redact transcripts/i,
    });
    expect((redactSwitch as HTMLInputElement).checked).toBe(false);
    fireEvent.click(redactSwitch);
    expect(setRedactTranscripts).toHaveBeenCalledWith(true);
  });

  it('enables "Jump to live" and calls onClickViewLatest when not viewing live', () => {
    const onClickViewLatest = vi.fn();
    render(
      <TranscriptActionsBar
        {...baseProps}
        onClickViewLatest={onClickViewLatest}
      />
    );
    const button = screen.getByRole('button', { name: /Jump to live/i });
    expect(button).not.toBeDisabled();
    fireEvent.click(button);
    expect(onClickViewLatest).toHaveBeenCalledTimes(1);
  });

  it('disables "Jump to live" when the view is already live', () => {
    render(<TranscriptActionsBar {...baseProps} disableJumpToLive />);
    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).toBeDisabled();
  });

  it('applies an alert filter selection immediately (no popover/apply)', () => {
    const setAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar {...baseProps} setAlertFilter={setAlertFilter} />
    );
    fireEvent.mouseDown(
      screen.getByRole('combobox', { name: /Transcript filter/i })
    );
    fireEvent.click(screen.getByRole('option', { name: 'Alerts only' }));
    expect(setAlertFilter).toHaveBeenCalledWith('alerts');
  });

  it('opens the date popover, commits the draft only on Apply', () => {
    const setDateTime = vi.fn();
    render(<TranscriptActionsBar {...baseProps} setDateTime={setDateTime} />);
    expect(screen.queryByText('set-draft-date')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByText('set-draft-date'));

    // Editing the draft alone doesn't apply — only Apply commits it.
    expect(setDateTime).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    expect(setDateTime).toHaveBeenCalledWith(PICKED_DATE);
  });

  it('Clear sets the date filter to null (returns to live)', () => {
    const setDateTime = vi.fn();
    render(
      <TranscriptActionsBar
        {...baseProps}
        dateTime={PICKED_DATE}
        setDateTime={setDateTime}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(setDateTime).toHaveBeenCalledWith(null);
  });

  it('Cancel closes the date popover without committing the draft', () => {
    const setDateTime = vi.fn();
    render(<TranscriptActionsBar {...baseProps} setDateTime={setDateTime} />);
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByText('set-draft-date'));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(setDateTime).not.toHaveBeenCalled();
  });

  it('Apply does not commit an invalid (partially typed) date', () => {
    const setDateTime = vi.fn();
    render(<TranscriptActionsBar {...baseProps} setDateTime={setDateTime} />);
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByText('set-invalid-date'));
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    expect(setDateTime).not.toHaveBeenCalled();
  });

  it('Apply is a no-op when the date is unchanged (no parent side effects)', () => {
    const setDateTime = vi.fn();
    render(
      <TranscriptActionsBar
        {...baseProps}
        dateTime={PICKED_DATE}
        setDateTime={setDateTime}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    expect(setDateTime).not.toHaveBeenCalled();
  });

  it('Clear is a no-op when no date filter is set', () => {
    const setDateTime = vi.fn();
    render(<TranscriptActionsBar {...baseProps} setDateTime={setDateTime} />);
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(setDateTime).not.toHaveBeenCalled();
  });

  it('commits the query after a typing pause (debounced)', () => {
    vi.useFakeTimers();
    try {
      const mockSetSearchQuery = vi.fn();
      render(
        <TranscriptActionsBar
          {...baseProps}
          searchQuery=""
          setSearchQuery={mockSetSearchQuery}
        />
      );

      const searchInput = screen.getByPlaceholderText(/Search transcripts/i);
      fireEvent.change(searchInput, { target: { value: 'fir' } });
      fireEvent.change(searchInput, { target: { value: 'fire' } });

      // Nothing commits mid-typing.
      vi.advanceTimersByTime(200);
      expect(mockSetSearchQuery).not.toHaveBeenCalled();

      // Only the final value commits, once, after the pause.
      vi.advanceTimersByTime(200);
      expect(mockSetSearchQuery).toHaveBeenCalledTimes(1);
      expect(mockSetSearchQuery).toHaveBeenCalledWith('fire');
    } finally {
      vi.useRealTimers();
    }
  });

  it('commits immediately on Enter and clears with the X button', () => {
    const mockSetSearchQuery = vi.fn();
    const { rerender } = render(
      <TranscriptActionsBar
        {...baseProps}
        searchQuery=""
        setSearchQuery={mockSetSearchQuery}
      />
    );

    const searchInput = screen.getByPlaceholderText(/Search transcripts/i);
    fireEvent.change(searchInput, { target: { value: 'fire' } });
    fireEvent.keyDown(searchInput, { key: 'Enter' });
    expect(mockSetSearchQuery).toHaveBeenCalledWith('fire');

    // Parent reflects the applied query; the X button resets it.
    rerender(
      <TranscriptActionsBar
        {...baseProps}
        searchQuery="fire"
        setSearchQuery={mockSetSearchQuery}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'clear search' }));
    expect(mockSetSearchQuery).toHaveBeenLastCalledWith('');
  });

  it('hides the clear button when the search field is empty', () => {
    render(<TranscriptActionsBar {...baseProps} searchQuery="" />);
    expect(screen.queryByRole('button', { name: 'clear search' })).toBeNull();
  });
});
