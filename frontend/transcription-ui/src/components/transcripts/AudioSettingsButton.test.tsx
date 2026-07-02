// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { AudioSettingsButton } from './AudioSettingsButton';

describe('AudioSettingsButton', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  const defaultProps = {
    volumeDb: 0,
    setVolumeDb: vi.fn(),
    pan: 0,
    setPan: vi.fn(),
    speed: 1,
    setSpeed: vi.fn(),
    activeSpeed: 1,
    accelerateNonSpeech: true,
    setAccelerateNonSpeech: vi.fn(),
    showAccelerateNonSpeech: false,
    onReset: vi.fn(),
    disableControls: false,
  };

  const renderButton = (overrides = {}) =>
    render(<AudioSettingsButton {...defaultProps} {...overrides} />);

  it('opens the audio controls popover with volume, pan, and speed', () => {
    renderButton({ volumeDb: -6 });

    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    expect(screen.getByText('Volume')).toBeTruthy();
    expect(screen.getByText('-6 dB')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Pan C' })).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Speed 1.5x' })).toBeTruthy();
  });

  it('applies pan and speed changes live', () => {
    const setPan = vi.fn();
    const setSpeed = vi.fn();
    renderButton({ setPan, setSpeed });

    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));
    fireEvent.click(screen.getByRole('button', { name: 'Pan R' }));
    fireEvent.click(screen.getByRole('button', { name: 'Speed 2x' }));

    expect(setPan).toHaveBeenCalledWith(1);
    expect(setSpeed).toHaveBeenCalledWith(2);
  });

  it('shows pan and speed badges on the speaker button when off-default', () => {
    renderButton({ pan: -1, speed: 1.5, activeSpeed: 1.5 });

    expect(screen.getByText('L')).toBeTruthy();
    expect(screen.getByText('1.5×')).toBeTruthy();
  });

  it('badges the live boosted rate while a non-speech clip accelerates', () => {
    // speed (user) stays 1, but the clip is playing at the boosted rate.
    renderButton({ speed: 1, activeSpeed: 6 });

    expect(screen.getByText('6×')).toBeTruthy();
  });

  it('shows the Fast Silence toggle only for continuous feeds', () => {
    const setAccelerateNonSpeech = vi.fn();
    const { rerender } = renderButton({ showAccelerateNonSpeech: false });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));
    expect(screen.queryByText('Fast Silence')).toBeNull();

    rerender(
      <AudioSettingsButton
        {...defaultProps}
        showAccelerateNonSpeech
        setAccelerateNonSpeech={setAccelerateNonSpeech}
      />
    );
    const checkbox = screen.getByRole('checkbox', {
      name: 'Fast Silence',
    });
    expect(checkbox).toBeTruthy();

    fireEvent.click(checkbox);
    expect(setAccelerateNonSpeech).toHaveBeenCalledWith(false);
  });

  it('selects no speed button while the boost is off the scale', () => {
    renderButton({ speed: 1, activeSpeed: 6 });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    expect(
      screen
        .getByRole('button', { name: 'Speed 1x' })
        .getAttribute('aria-pressed')
    ).toBe('false');
  });

  it('picking a speed while accelerating stops acceleration and applies it', () => {
    const setSpeed = vi.fn();
    const setAccelerateNonSpeech = vi.fn();
    renderButton({
      speed: 1,
      activeSpeed: 6,
      setSpeed,
      setAccelerateNonSpeech,
    });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    fireEvent.click(screen.getByRole('button', { name: 'Speed 1x' }));

    expect(setSpeed).toHaveBeenCalledWith(1);
    expect(setAccelerateNonSpeech).toHaveBeenCalledWith(false);
  });

  it('leaves acceleration alone when changing speed on a speech clip', () => {
    const setAccelerateNonSpeech = vi.fn();
    renderButton({ speed: 1, activeSpeed: 1, setAccelerateNonSpeech });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    fireEvent.click(screen.getByRole('button', { name: 'Speed 1.5x' }));

    expect(setAccelerateNonSpeech).not.toHaveBeenCalled();
  });

  it('shows no badges when controls are at their defaults', () => {
    renderButton();

    expect(screen.queryByText('L')).toBeNull();
    expect(screen.queryByText('R')).toBeNull();
    expect(screen.getByTestId('VolumeUpIcon')).toBeTruthy();
  });

  it('only swaps the speaker icon for mute; otherwise keeps the volume icon', () => {
    const { rerender } = renderButton({ volumeDb: -6 });
    // A cut still shows the volume icon — the icon scale conveys direction.
    expect(screen.getByTestId('VolumeUpIcon')).toBeTruthy();

    rerender(<AudioSettingsButton {...defaultProps} volumeDb={-30} />);
    expect(screen.getByTestId('VolumeOffIcon')).toBeTruthy();
  });

  it('asks to reset all controls when "Reset" is clicked', () => {
    const onReset = vi.fn();
    renderButton({ volumeDb: -6, pan: 1, speed: 1.5, onReset });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    fireEvent.click(screen.getByRole('button', { name: 'Reset' }));

    expect(onReset).toHaveBeenCalledTimes(1);
  });

  it('keeps keyboard volume steps exact inside the snap zone', () => {
    const setVolumeDb = vi.fn();
    renderButton({ volumeDb: 2, setVolumeDb });
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    fireEvent.keyDown(screen.getByRole('slider', { name: 'Volume' }), {
      key: 'ArrowDown',
    });

    // 2 → 1 by one step. A drag would snap 1 to the default (0); the keyboard
    // path must land on 1, proving the snap is bypassed and ±1 dB stays reachable.
    expect(setVolumeDb).toHaveBeenCalledWith(1);
  });

  it('disables the speed control for Safari users', () => {
    vi.stubGlobal('navigator', {
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    });

    renderButton();
    fireEvent.click(screen.getByRole('button', { name: 'audio controls' }));

    expect(screen.getByRole('button', { name: 'Speed 1x' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Pan C' })).not.toBeDisabled();
  });

  it('disables the button when disableControls is true', () => {
    renderButton({ disableControls: true });
    expect(
      screen.getByRole('button', { name: 'audio controls' })
    ).toBeDisabled();
  });
});
