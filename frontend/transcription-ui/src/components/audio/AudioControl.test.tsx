// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { AudioControl } from './AudioControl';

describe('AudioControl', () => {
  const mockOnTogglePlayPause = vi.fn();
  const mockOnSkipToNext = vi.fn();
  const mockOnSkipToPrevious = vi.fn();
  const mockOnFastForward = vi.fn();
  const mockOnFastRewind = vi.fn();
  const mockOnSkipTime = vi.fn();

  const defaultProps = {
    isAudioPlaying: false,
    onTogglePlayPause: mockOnTogglePlayPause,
    onSkipToNext: mockOnSkipToNext,
    onSkipToPrevious: mockOnSkipToPrevious,
    onFastForward: mockOnFastForward,
    onFastRewind: mockOnFastRewind,
    onSkipTime: mockOnSkipTime,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('renders all buttons with correct aria-labels', () => {
    render(<AudioControl {...defaultProps} />);

    expect(
      screen.getByLabelText('rewind to previous detected speech')
    ).toBeTruthy();
    expect(screen.getByLabelText('rewind to previous segment')).toBeTruthy();
    expect(screen.getByLabelText('rewind 5 seconds')).toBeTruthy();
    expect(screen.getByLabelText('play')).toBeTruthy();
    expect(screen.getByLabelText('advance 5 seconds')).toBeTruthy();
    expect(screen.getByLabelText('advance to next segment')).toBeTruthy();
    expect(
      screen.getByLabelText('advance to next detected speech')
    ).toBeTruthy();
  });

  it('renders volume slider and Audio controls button in row when state is passed', () => {
    render(
      <AudioControl
        {...defaultProps}
        volumeDb={0}
        setVolumeDb={vi.fn()}
        pan={0}
        setPan={vi.fn()}
        speed={1}
        setSpeed={vi.fn()}
      />
    );

    expect(screen.getByRole('slider', { name: 'Volume' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'audio controls' })).toBeTruthy();
  });

  it('renders active Pan and Speed Chip badges right of settings button when off-default', () => {
    const setPan = vi.fn();
    const setSpeed = vi.fn();
    render(
      <AudioControl
        {...defaultProps}
        volumeDb={0}
        setVolumeDb={vi.fn()}
        pan={-1}
        setPan={setPan}
        speed={1.5}
        setSpeed={setSpeed}
      />
    );

    expect(screen.getByText('Pan L')).toBeTruthy();
    expect(screen.getByText('1.5×')).toBeTruthy();

    // Clicking delete button on Pan chip resets pan to 0
    const panCancel = screen.getAllByTestId('CancelIcon')[0];
    fireEvent.click(panCancel);
    expect(setPan).toHaveBeenCalledWith(0);
  });

  it('shows pause icon when isAudioPlaying is true', () => {
    render(<AudioControl {...defaultProps} isAudioPlaying={true} />);
    expect(screen.getByLabelText('pause')).toBeTruthy();
    expect(screen.queryByLabelText('play')).toBeNull();
  });

  it('triggers callback for rewind to previous detected speech button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(
      screen.getByLabelText('rewind to previous detected speech')
    );
    expect(mockOnFastRewind).toHaveBeenCalledTimes(1);
  });

  it('triggers callback for rewind to previous segment button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('rewind to previous segment'));
    expect(mockOnSkipToPrevious).toHaveBeenCalledTimes(1);
  });

  it('triggers callback for rewind 5 seconds button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('rewind 5 seconds'));
    expect(mockOnSkipTime).toHaveBeenCalledWith(-5);
  });

  it('triggers callback for play/pause button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('play'));
    expect(mockOnTogglePlayPause).toHaveBeenCalledTimes(1);
  });

  it('triggers callback for advance 5 seconds button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('advance 5 seconds'));
    expect(mockOnSkipTime).toHaveBeenCalledWith(5);
  });

  it('triggers callback for advance to next segment button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('advance to next segment'));
    expect(mockOnSkipToNext).toHaveBeenCalledTimes(1);
  });

  it('triggers callback for advance to next detected speech button', () => {
    render(<AudioControl {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('advance to next detected speech'));
    expect(mockOnFastForward).toHaveBeenCalledTimes(1);
  });

  it('disables all buttons when disableControls is true', () => {
    render(<AudioControl {...defaultProps} disableControls={true} />);

    expect(
      screen.getByLabelText('rewind to previous detected speech')
    ).toBeDisabled();
    expect(screen.getByLabelText('rewind to previous segment')).toBeDisabled();
    expect(screen.getByLabelText('rewind 5 seconds')).toBeDisabled();
    expect(screen.getByLabelText('play')).toBeDisabled();
    expect(screen.getByLabelText('advance 5 seconds')).toBeDisabled();
    expect(screen.getByLabelText('advance to next segment')).toBeDisabled();
    expect(
      screen.getByLabelText('advance to next detected speech')
    ).toBeDisabled();
  });
});
