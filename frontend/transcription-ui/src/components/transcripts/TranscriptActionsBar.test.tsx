// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

// MUI's desktop picker can't be driven through its segmented inputs under JSDOM,
// so mock it as a minimal harness that surfaces the onAccept wiring: an "apply"
// button accepts a fixed date, a "clear" button accepts null (Clear → live).
const PICKED_DATE = new Date('2026-05-14T12:00:00.000Z');
vi.mock('@mui/x-date-pickers/DesktopDateTimePicker', () => ({
  DesktopDateTimePicker: (props: {
    open: boolean;
    onAccept: (value: Date | null) => void;
  }) =>
    props.open ? (
      <div data-testid="mock-picker">
        <button onClick={() => props.onAccept(PICKED_DATE)}>
          picker-apply
        </button>
        <button onClick={() => props.onAccept(null)}>picker-clear</button>
      </div>
    ) : null,
}));
vi.mock('@mui/x-date-pickers/LocalizationProvider', () => ({
  LocalizationProvider: ({ children }: { children: React.ReactNode }) =>
    children,
}));
vi.mock('@mui/x-date-pickers/AdapterDateFns', () => ({
  AdapterDateFns: class {},
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

  it('opens the date picker from the calendar button and applies the pick', () => {
    const setDateTime = vi.fn();
    render(<TranscriptActionsBar {...baseProps} setDateTime={setDateTime} />);
    expect(screen.queryByTestId('mock-picker')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    expect(screen.getByTestId('mock-picker')).toBeTruthy();

    fireEvent.click(screen.getByText('picker-apply'));
    expect(setDateTime).toHaveBeenCalledWith(PICKED_DATE);
  });

  it('clearing the picker sets the date filter to null (returns to live)', () => {
    const setDateTime = vi.fn();
    render(
      <TranscriptActionsBar
        {...baseProps}
        dateTime={PICKED_DATE}
        setDateTime={setDateTime}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(screen.getByText('picker-clear'));
    expect(setDateTime).toHaveBeenCalledWith(null);
  });

  it('opens the search popover, applies the query on Apply, and clears it', () => {
    const mockSetSearchQuery = vi.fn();
    const { rerender } = render(
      <TranscriptActionsBar
        disableJumpToLive={false}
        redactTranscripts={false}
        setRedactTranscripts={vi.fn()}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
        searchQuery=""
        setSearchQuery={mockSetSearchQuery}
      />
    );

    // Open the search popover from the magnifier button.
    fireEvent.click(screen.getByRole('button', { name: 'search' }));

    const searchInput = screen.getByPlaceholderText(/Search transcripts/i);
    fireEvent.change(searchInput, { target: { value: 'fire' } });

    // Typing alone doesn't apply the query — only Apply commits it.
    expect(mockSetSearchQuery).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    expect(mockSetSearchQuery).toHaveBeenCalledWith('fire');

    // Parent reflects the applied query; Clear resets it.
    rerender(
      <TranscriptActionsBar
        disableJumpToLive={false}
        redactTranscripts={false}
        setRedactTranscripts={vi.fn()}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
        searchQuery="fire"
        setSearchQuery={mockSetSearchQuery}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'search' }));
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(mockSetSearchQuery).toHaveBeenLastCalledWith('');
  });
});
