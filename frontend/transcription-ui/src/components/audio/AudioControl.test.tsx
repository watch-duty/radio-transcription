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
    showPauseIcon: false,
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

  it('shows pause icon when showPauseIcon is true', () => {
    render(<AudioControl {...defaultProps} showPauseIcon={true} />);
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
    expect(mockOnSkipTime).toHaveBeenCalledTimes(1);
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
