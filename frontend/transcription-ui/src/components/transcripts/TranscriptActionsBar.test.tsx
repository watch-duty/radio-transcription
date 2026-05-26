// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

// Mock the DateTimePicker because Material UI's modern segment-based picker inputs
// do not support standard text value setters under JSDOM, which throws:
// "Error: The given element does not have a value setter" when using fireEvent.change().
// Mocking it as a simple native input allows fast, isolated, and robust testing.
vi.mock('../common/DateTimePicker', () => ({
  DateTimePicker: (props: {
    dateTime: Date | null;
    setDateTime: (dateTime: Date | null) => void;
  }) => (
    <input
      aria-label="Date/time"
      value={props.dateTime ? props.dateTime.toISOString() : ''}
      onChange={(e) => {
        props.setDateTime(e.target.value ? new Date(e.target.value) : null);
      }}
    />
  ),
}));

describe('TranscriptActionsBar', () => {
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

  it('renders the redact switch and toggles state when changed', () => {
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        onClickViewLatest={vi.fn()}
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

  it('renders "Jump to live" button when hasNewerTranscripts is true and calls onClickViewLatest when clicked', () => {
    const onClickViewLatest = vi.fn();
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={true}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        onClickViewLatest={onClickViewLatest}
      />
    );

    const button = screen.getByRole('button', { name: /Jump to live/i });
    expect(button).toBeTruthy();
    expect(button).not.toBeDisabled();

    fireEvent.click(button);
    expect(onClickViewLatest).toHaveBeenCalledTimes(1);
  });

  it('renders disabled "Jump to live" button when hasNewerTranscripts is false', () => {
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    const button = screen.getByRole('button', { name: /Jump to live/i });
    expect(button).toBeTruthy();
    expect(button).toBeDisabled();
  });

  it('opens the filter menu, clears local value, and applies new value on Apply', () => {
    const mockSetDateTime = vi.fn();
    render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
        onClickViewLatest={vi.fn()}
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
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
        onClickViewLatest={vi.fn()}
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

    expect(mockSetDateTime).toHaveBeenCalledWith(
      new Date('2026-05-14T12:00:00Z')
    );
  });

  it('calls setDateTime with the selected date on Apply, and calls setDateTime with null when the date/time filter chip is deleted', () => {
    const mockSetDateTime = vi.fn();
    const { rerender } = render(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={mockSetDateTime}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(screen.queryByTestId('CancelIcon')).toBeNull();

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const dateInput = screen.getAllByLabelText('Date/time')[0];
    fireEvent.change(dateInput, {
      target: { value: '2026-05-14T12:00:00.000Z' },
    });

    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetDateTime).toHaveBeenCalledWith(
      new Date('2026-05-14T12:00:00.000Z')
    );

    // Simulate the parent component updating the prop in response to the setDateTime call
    rerender(
      <TranscriptActionsBar
        searchedTimestamp={null}
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00.000Z')}
        setDateTime={mockSetDateTime}
        onClickViewLatest={vi.fn()}
      />
    );

    const deleteIcon = screen.getByTestId('CancelIcon');
    expect(deleteIcon).toBeTruthy();

    fireEvent.click(deleteIcon);
    expect(mockSetDateTime).toHaveBeenLastCalledWith(null);
  });
});
