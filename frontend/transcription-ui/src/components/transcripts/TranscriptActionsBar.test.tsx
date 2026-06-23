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

const audioControlProps = {
  volumeDb: 0,
  setVolumeDb: vi.fn(),
  pan: 0,
  setPan: vi.fn(),
  speed: 1,
  setSpeed: vi.fn(),
};

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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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

  it('renders disabled "Jump to live" button when hasNewerAudioSegments is false', () => {
    render(
      <TranscriptActionsBar
        {...audioControlProps}
        searchedTimestamp={null}
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

  it('opens the filter menu, clears local value, and applies new value on Apply', () => {
    const mockSetDateTime = vi.fn();
    const mockSetAlertFilter = vi.fn();
    render(
      <TranscriptActionsBar
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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
        {...audioControlProps}
        searchedTimestamp={null}
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

  const renderBar = (overrides = {}) =>
    render(
      <TranscriptActionsBar
        {...audioControlProps}
        {...overrides}
        searchedTimestamp={null}
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

  it('opens the audio controls popover with volume, pan, and speed', () => {
    renderBar({ volumeDb: -6 });

    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    expect(screen.getByText('Volume')).toBeTruthy();
    expect(screen.getByText('-6 dB')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Pan C' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Speed 1.5x' })).toBeTruthy();
  });

  it('applies pan and speed changes live', () => {
    const setPan = vi.fn();
    const setSpeed = vi.fn();
    renderBar({ setPan, setSpeed });

    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));
    fireEvent.click(screen.getByRole('button', { name: 'Pan R' }));
    fireEvent.click(screen.getByRole('button', { name: 'Speed 2x' }));

    expect(setPan).toHaveBeenCalledWith(1);
    expect(setSpeed).toHaveBeenCalledWith(2);
  });

  it('shows pan and speed badges on the speaker button when off-default', () => {
    renderBar({ pan: -1, speed: 1.5 });

    expect(screen.getByText('L')).toBeTruthy();
    expect(screen.getByText('1.5×')).toBeTruthy();
  });

  it('shows no badges when controls are at their defaults', () => {
    renderBar();

    expect(screen.queryByText('L')).toBeNull();
    expect(screen.queryByText('R')).toBeNull();
    expect(screen.getByTestId('VolumeUpIcon')).toBeTruthy();
  });

  it('reflects volume direction in the speaker icon', () => {
    const { rerender } = renderBar({ volumeDb: -6 });
    expect(screen.getByTestId('VolumeDownIcon')).toBeTruthy();

    rerender(
      <TranscriptActionsBar
        {...audioControlProps}
        volumeDb={-30}
        searchedTimestamp={null}
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
    expect(screen.getByTestId('VolumeOffIcon')).toBeTruthy();
  });

  it('shows a reset control only when volume is off default, and resets to 0', () => {
    const setVolumeDb = vi.fn();
    const { rerender } = renderBar({ volumeDb: 0, setVolumeDb });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));
    expect(screen.queryByRole('button', { name: 'Reset volume' })).toBeNull();

    rerender(
      <TranscriptActionsBar
        {...audioControlProps}
        volumeDb={-6}
        setVolumeDb={setVolumeDb}
        searchedTimestamp={null}
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

    fireEvent.click(screen.getByRole('button', { name: 'Reset volume' }));
    expect(setVolumeDb).toHaveBeenCalledWith(0);
  });

  it('keeps keyboard volume steps exact near the default', () => {
    const setVolumeDb = vi.fn();
    renderBar({ volumeDb: 1, setVolumeDb });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    fireEvent.keyDown(screen.getByRole('slider', { name: 'Volume' }), {
      key: 'ArrowDown',
    });

    // 1 → 0 by one step; the keyboard path is not snapped, so it lands on 0
    // through the step itself, not a snap, and -1 stays reachable.
    expect(setVolumeDb).toHaveBeenCalledWith(0);
  });

  it('disables the speed control for Safari users', () => {
    vi.stubGlobal('navigator', {
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    });

    renderBar();
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pan C' })).not.toBeDisabled();
  });
});
