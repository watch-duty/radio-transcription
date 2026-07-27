// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { PanControl } from './PanControl';

describe('PanControl', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const defaultProps = {
    pan: 0,
    setPan: vi.fn(),
    disableControls: false,
  };

  it('renders L, C, R pan buttons', () => {
    render(<PanControl {...defaultProps} />);

    expect(screen.getByRole('button', { name: 'Pan L' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Pan C' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Pan R' })).toBeTruthy();
  });

  it('calls setPan when a pan option is selected', () => {
    const setPan = vi.fn();
    render(<PanControl {...defaultProps} setPan={setPan} />);

    fireEvent.click(screen.getByRole('button', { name: 'Pan R' }));
    expect(setPan).toHaveBeenCalledWith(1);
  });

  it('disables buttons when disableControls is true', () => {
    render(<PanControl {...defaultProps} disableControls={true} />);

    expect(screen.getByRole('button', { name: 'Pan L' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pan C' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pan R' })).toBeDisabled();
  });
});
