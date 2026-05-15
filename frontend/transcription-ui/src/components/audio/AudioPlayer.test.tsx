// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import AudioPlayer from './AudioPlayer';

describe('AudioPlayer', () => {
  const mockOnToggleAudio = vi.fn();
  const defaultProps = {
    audioUri: 'gs://bucket/audio.mp3',
    transmissionId: '123',
    onToggleAudio: mockOnToggleAudio,
    isAudioPlaying: false,
    currentlyPlayingTransmissionId: null,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('renders play button initially', () => {
    render(<AudioPlayer {...defaultProps} />);
    expect(screen.getByLabelText('play')).toBeTruthy();
  });

  it('calls onToggleAudio when clicked', () => {
    render(<AudioPlayer {...defaultProps} />);

    const button = screen.getByLabelText('play');
    fireEvent.click(button);

    expect(mockOnToggleAudio).toHaveBeenCalledWith(
      '123',
      'gs://bucket/audio.mp3'
    );
  });

  it('renders pause button when playing this transmission', () => {
    render(
      <AudioPlayer
        {...defaultProps}
        currentlyPlayingTransmissionId="123"
        isAudioPlaying={true}
      />
    );
    expect(screen.getByLabelText('pause')).toBeTruthy();
  });

  it('renders play button when playing another transmission', () => {
    render(
      <AudioPlayer
        {...defaultProps}
        currentlyPlayingTransmissionId="456"
        isAudioPlaying={true}
      />
    );
    expect(screen.getByLabelText('play')).toBeTruthy();
  });

  it('renders play button when not playing even if currentlyPlayingTransmissionId matches', () => {
    render(
      <AudioPlayer
        {...defaultProps}
        currentlyPlayingTransmissionId="123"
        isAudioPlaying={false}
      />
    );
    expect(screen.getByLabelText('play')).toBeTruthy();
  });
});
