// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import AudioPlayer from './AudioPlayer';

let mockCapturedOptions: any;
const mockHowlInstance = {
  play: vi.fn(),
  pause: vi.fn(),
  stop: vi.fn(),
  unload: vi.fn(),
};

vi.mock('howler', () => ({
  Howl: function (opts: any) {
    mockCapturedOptions = opts;
    return mockHowlInstance;
  },
}));

describe('AudioPlayer', () => {
  const mockOnPlay = vi.fn();
  const defaultProps = {
    audioUri: 'gs://bucket/audio.mp3',
    transmissionId: '123',
    onPlay: mockOnPlay,
    currentlyPlayingTransmissionId: null,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockCapturedOptions = null;
  });

  afterEach(() => {
    cleanup();
  });

  it('renders play button initially', () => {
    render(<AudioPlayer {...defaultProps} />);
    expect(screen.getByLabelText('play')).toBeTruthy();
  });

  it('calls onPlay and plays audio when clicked', () => {
    render(<AudioPlayer {...defaultProps} />);
    
    const button = screen.getByLabelText('play');
    fireEvent.click(button);

    expect(mockOnPlay).toHaveBeenCalledWith('123');
    expect(mockHowlInstance.play).toHaveBeenCalled();
  });

  it('updates UI to pause when playing', () => {
    render(<AudioPlayer {...defaultProps} />);
    
    // Simulate onplay event
    act(() => {
      mockCapturedOptions.onplay();
    });
    
    expect(screen.getByLabelText('pause')).toBeTruthy();
  });

  it('calls pause when clicked while playing', () => {
    render(<AudioPlayer {...defaultProps} />);
    
    // Simulate playing state
    act(() => {
      mockCapturedOptions.onplay();
    });
    
    const button = screen.getByLabelText('pause');
    fireEvent.click(button);

    expect(mockHowlInstance.pause).toHaveBeenCalled();
  });

  it('stops audio when another transmission starts playing', () => {
    const { rerender } = render(<AudioPlayer {...defaultProps} />);
    
    // Simulate playing
    act(() => {
      mockCapturedOptions.onplay();
    });
    
    // Rerender with a different currentlyPlayingTransmissionId
    rerender(
      <AudioPlayer
        {...defaultProps}
        currentlyPlayingTransmissionId="456"
      />
    );

    expect(mockHowlInstance.stop).toHaveBeenCalled();
  });

  it('unloads sound on unmount', () => {
    const { unmount } = render(<AudioPlayer {...defaultProps} />);
    
    unmount();

    expect(mockHowlInstance.unload).toHaveBeenCalled();
  });
});
