// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import TranscriptPlayControl from './TranscriptPlayControl';

describe('AudioPlayer', () => {
  const mockOnToggleAudio = vi.fn();
  const defaultProps = {
    audioUri: 'gs://bucket/audio.mp3',
    segmentId: '123',
    onToggleAudio: mockOnToggleAudio,
    isAudioPlaying: false,
    currentlyPlayingSegmentId: null,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('renders play button initially', () => {
    render(<TranscriptPlayControl {...defaultProps} />);
    expect(screen.getByLabelText('play')).toBeTruthy();
  });

  it('calls onToggleAudio when clicked', () => {
    render(<TranscriptPlayControl {...defaultProps} />);

    const button = screen.getByLabelText('play');
    fireEvent.click(button);

    expect(mockOnToggleAudio).toHaveBeenCalledWith(
      '123',
      'gs://bucket/audio.mp3'
    );
  });

  it('renders pause button when playing this segment', () => {
    render(
      <TranscriptPlayControl
        {...defaultProps}
        currentlyPlayingSegmentId="123"
        isAudioPlaying={true}
      />
    );
    expect(screen.getByLabelText('pause')).toBeTruthy();
  });

  it('renders play button when playing another segment', () => {
    render(
      <TranscriptPlayControl
        {...defaultProps}
        currentlyPlayingSegmentId="456"
        isAudioPlaying={true}
      />
    );
    expect(screen.getByLabelText('play')).toBeTruthy();
  });

  it('renders play button when not playing even if currentlyPlayingSegmentId matches', () => {
    render(
      <TranscriptPlayControl
        {...defaultProps}
        currentlyPlayingSegmentId="123"
        isAudioPlaying={false}
      />
    );
    expect(screen.getByLabelText('play')).toBeTruthy();
  });
});
