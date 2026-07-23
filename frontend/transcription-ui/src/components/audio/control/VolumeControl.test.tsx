// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { VolumeControl } from './VolumeControl';

describe('VolumeControl', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const defaultProps = {
    volumeDb: 0,
    setVolumeDb: vi.fn(),
    disableControls: false,
  };

  it('renders volume icon and slider', () => {
    render(<VolumeControl {...defaultProps} volumeDb={-6} />);

    expect(screen.getByTestId('VolumeUpIcon')).toBeTruthy();
    expect(screen.getByRole('slider', { name: 'Volume' })).toBeTruthy();
  });

  it('renders VolumeOffIcon when volume is muted', () => {
    render(<VolumeControl {...defaultProps} volumeDb={-30} />);

    expect(screen.getByTestId('VolumeOffIcon')).toBeTruthy();
  });

  it('keeps keyboard volume steps exact inside the snap zone', () => {
    const setVolumeDb = vi.fn();
    render(
      <VolumeControl {...defaultProps} volumeDb={2} setVolumeDb={setVolumeDb} />
    );

    fireEvent.keyDown(screen.getByRole('slider', { name: 'Volume' }), {
      key: 'ArrowDown',
    });

    expect(setVolumeDb).toHaveBeenCalledWith(1);
  });

  it('disables slider when disableControls is true', () => {
    render(<VolumeControl {...defaultProps} disableControls={true} />);

    expect(screen.getByRole('slider', { name: 'Volume' })).toBeDisabled();
  });
});
