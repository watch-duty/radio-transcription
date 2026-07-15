// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { AudioSettingsMenu } from './AudioSettingsMenu';

describe('AudioSettingsMenu', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const defaultProps = {
    pan: 0,
    setPan: vi.fn(),
    speed: 1,
    setSpeed: vi.fn(),
    onReset: vi.fn(),
    disableControls: false,
  };

  it('renders "audio controls" button and opens popover menu on click', () => {
    render(<AudioSettingsMenu {...defaultProps} />);

    const button = screen.getByRole('button', { name: 'audio controls' });
    expect(button).toBeTruthy();

    fireEvent.click(button);

    expect(screen.getByText('Pan')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Pan C' })).toBeTruthy();
    expect(screen.getByText('Speed')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeTruthy();
  });

  it('calls onReset when Reset button inside popover is clicked', () => {
    const onReset = vi.fn();
    render(<AudioSettingsMenu {...defaultProps} onReset={onReset} />);

    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));
    fireEvent.click(screen.getByRole('button', { name: 'Reset' }));

    expect(onReset).toHaveBeenCalledTimes(1);
  });

  it('disables "audio controls" button when disableControls is true', () => {
    render(<AudioSettingsMenu {...defaultProps} disableControls={true} />);

    expect(
      screen.getByRole('button', { name: 'audio controls' })
    ).toBeDisabled();
  });
});
