// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { formatClockTime } from '../../utils/timeUtils';
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
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
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

  it('renders "Jump to live" button when hasNewerAudioSegments is true and calls onClickViewLatest when clicked', () => {
    const onClickViewLatest = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={true}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={onClickViewLatest}
      />
    );

    const button = screen.getByRole('button', { name: /Jump to live/i });
    expect(button).toBeTruthy();
    expect(button).not.toBeDisabled();

    fireEvent.click(button);
    expect(onClickViewLatest).toHaveBeenCalledTimes(1);
  });

  it('renders disabled "Jump to live" button when at the live edge', () => {
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    const button = screen.getByRole('button', { name: /Jump to live/i });
    expect(button).toBeTruthy();
    expect(button).toBeDisabled();
  });

  it('enables "Jump to live" at the live window when playback is not live (paused or a stale playhead)', () => {
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        isPlaybackAtLiveEdge={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).not.toBeDisabled();
  });

  it('enables "Jump to live" when viewing back, even without newer segments', () => {
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        isLatestTimeWindow={false}
        activeWindowTime={new Date('2026-05-14T13:05:00Z').getTime()}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).not.toBeDisabled();
  });

  it('enables "Jump to live" when a date filter is active', () => {
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T13:05:00Z')}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).not.toBeDisabled();
  });

  it('shows the past window time in the chip when viewing back without a date filter', () => {
    const windowMs = new Date('2026-05-14T13:05:00Z').getTime();
    const { container } = render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        isLatestTimeWindow={false}
        activeWindowTime={windowMs}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(screen.getByText('Viewing:')).toBeTruthy();
    expect(container.textContent).toContain(formatClockTime(windowMs));
  });

  it('formats the date/time chip in 24-hour time (no AM/PM)', () => {
    const dt = new Date('2026-05-14T13:05:00Z');
    const { container } = render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={dt}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    expect(container.textContent).toContain(formatClockTime(dt.getTime()));
    expect(container.textContent).not.toMatch(/\b[AP]M\b/);
  });

  it('opens the filter menu, clears local value, and applies new value on Apply', () => {
    const mockSetDateTime = vi.fn();
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
        alertFilter="all"
        setAlertFilter={mockSetAlertFilter}
        onClickViewLatest={vi.fn()}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const clearButton = screen.getByRole('button', { name: 'Clear' });
    fireEvent.click(clearButton);

    // Should not have applied yet
    expect(mockSetDateTime).not.toHaveBeenCalled();
    expect(mockSetAlertFilter).not.toHaveBeenCalled();

    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetDateTime).toHaveBeenCalledWith(null);
    expect(mockSetAlertFilter).toHaveBeenCalledWith('all');
  });

  it('keeps the original value if cleared and then cancelled', () => {
    const mockSetDateTime = vi.fn();
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00Z')}
        setDateTime={mockSetDateTime}
        alertFilter="all"
        setAlertFilter={mockSetAlertFilter}
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
    expect(mockSetAlertFilter).not.toHaveBeenCalled();

    // Reopen filter menu and apply to verify local state reverted to prop value
    fireEvent.click(filterButton);
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetDateTime).toHaveBeenCalledWith(
      new Date('2026-05-14T12:00:00Z')
    );
    expect(mockSetAlertFilter).toHaveBeenCalledWith('all');
  });

  it('calls setDateTime with the selected date on Apply, and calls setDateTime with null when the date/time filter chip is deleted', () => {
    const mockSetDateTime = vi.fn();
    const { rerender } = render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={mockSetDateTime}
        alertFilter="all"
        setAlertFilter={vi.fn()}
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
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date('2026-05-14T12:00:00.000Z')}
        setDateTime={mockSetDateTime}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    const deleteIcon = screen.getByTestId('CancelIcon');
    expect(deleteIcon).toBeTruthy();

    fireEvent.click(deleteIcon);
    expect(mockSetDateTime).toHaveBeenLastCalledWith(null);
  });

  it('opens filter menu, shows the alerts filter dropdown, selects Alerts Only, and applies the filter', () => {
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={mockSetAlertFilter}
        onClickViewLatest={vi.fn()}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const selectLabel = screen.getByLabelText('Show');
    expect(selectLabel).toBeTruthy();

    const selectTrigger = screen.getByRole('combobox', { name: /Show/i });
    fireEvent.mouseDown(selectTrigger);

    const option = screen.getByRole('option', { name: 'Alerts only' });
    fireEvent.click(option);

    // Before apply, setAlertFilter should not have been called
    expect(mockSetAlertFilter).not.toHaveBeenCalled();

    // Click apply
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetAlertFilter).toHaveBeenCalledWith('alerts');
  });

  it('keeps the original alert filter if cancelled', () => {
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={mockSetAlertFilter}
        onClickViewLatest={vi.fn()}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const selectTrigger = screen.getByRole('combobox', { name: /Show/i });
    fireEvent.mouseDown(selectTrigger);

    const option = screen.getByRole('option', { name: 'Alerts only' });
    fireEvent.click(option);

    // Click cancel
    const cancelButton = screen.getByRole('button', { name: 'Cancel' });
    fireEvent.click(cancelButton);

    expect(mockSetAlertFilter).not.toHaveBeenCalled();

    // Re-open and verify it still applies original value
    fireEvent.click(filterButton);
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetAlertFilter).toHaveBeenCalledWith('all');
  });

  it('resets alert filter to all when cleared in popover', () => {
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="alerts"
        setAlertFilter={mockSetAlertFilter}
        onClickViewLatest={vi.fn()}
      />
    );

    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    const clearButton = screen.getByRole('button', { name: 'Clear' });
    fireEvent.click(clearButton);

    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    expect(mockSetAlertFilter).toHaveBeenCalledWith('all');
  });

  it('displays the alerts chip when filter is active and handles deleting the chip', () => {
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="alerts"
        setAlertFilter={mockSetAlertFilter}
        onClickViewLatest={vi.fn()}
      />
    );

    const chipText = screen.getByText('Show:');
    expect(chipText).toBeTruthy();
    expect(screen.getByText('Alerts only')).toBeTruthy();

    const deleteIcon = screen.getByTestId('CancelIcon');
    fireEvent.click(deleteIcon);

    expect(mockSetAlertFilter).toHaveBeenCalledWith('all');
  });

  it('updates the filter badge content correctly based on active filters count', () => {
    const { rerender } = render(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );

    // No filters active, badge should be invisible (with count 0)
    const zeroBadge = screen.getByText('0');
    expect(zeroBadge).toBeTruthy();
    expect(zeroBadge.className).toContain('MuiBadge-invisible');

    // Rerender with date filter only
    rerender(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date()}
        setDateTime={vi.fn()}
        alertFilter="all"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );
    const oneBadge = screen.getByText('1');
    expect(oneBadge).toBeTruthy();
    expect(oneBadge.className).not.toContain('MuiBadge-invisible');

    // Rerender with alerts filter only
    rerender(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={null}
        setDateTime={vi.fn()}
        alertFilter="alerts"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );
    const oneBadgeAlerts = screen.getByText('1');
    expect(oneBadgeAlerts).toBeTruthy();
    expect(oneBadgeAlerts.className).not.toContain('MuiBadge-invisible');

    // Rerender with both filters active
    rerender(
      <TranscriptActionsBar
        hasNewerAudioSegments={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        dateTime={new Date()}
        setDateTime={vi.fn()}
        alertFilter="alerts"
        setAlertFilter={vi.fn()}
        onClickViewLatest={vi.fn()}
      />
    );
    const twoBadge = screen.getByText('2');
    expect(twoBadge).toBeTruthy();
    expect(twoBadge.className).not.toContain('MuiBadge-invisible');
  });
});
