// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { SpeedControl } from './SpeedControl';

describe('SpeedControl', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const defaultProps = {
    speed: 1,
    setSpeed: vi.fn(),
    disableControls: false,
  };

  it('renders speed buttons', () => {
    render(<SpeedControl {...defaultProps} />);

    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Speed 1.5x' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Speed 2x' })).toBeTruthy();
  });

  it('calls setSpeed when a speed option is selected', () => {
    const setSpeed = vi.fn();
    render(<SpeedControl {...defaultProps} setSpeed={setSpeed} />);

    fireEvent.click(screen.getByRole('button', { name: 'Speed 1.5x' }));
    expect(setSpeed).toHaveBeenCalledWith(1.5);
  });

  it('disables the speed control for Safari users', () => {
    vi.stubGlobal('navigator', {
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    });

    render(<SpeedControl {...defaultProps} />);

    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeDisabled();
  });

  it('disables buttons when disableControls is true', () => {
    render(<SpeedControl {...defaultProps} disableControls={true} />);

    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeDisabled();
  });
});
