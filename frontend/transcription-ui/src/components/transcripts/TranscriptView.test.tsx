// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import { listTranscripts } from '../../service/listTranscripts';
import TranscriptView from './TranscriptView';

// Mock the service
vi.mock('../../service/listTranscripts', () => ({
  listTranscripts: vi.fn(),
}));

// Mock AuthContext
vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-token' }),
}));

describe('TranscriptView', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('renders search field and fetch button', () => {
    render(<TranscriptView />);
    expect(screen.getByLabelText(/Enter Feed ID/i)).toBeTruthy();
    expect(screen.getByRole('button', { name: /Fetch/i })).toBeTruthy();
  });

  it('shows loading state when fetching', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce([]);

    render(<TranscriptView />);

    const input = screen.getByLabelText(/Enter Feed ID/i);
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    expect((button as HTMLButtonElement).disabled).toBe(true);

    await waitFor(() => {
      expect((button as HTMLButtonElement).disabled).toBe(false);
    });
  });

  it('renders transcripts when fetched', async () => {
    const mockTranscripts = [
      {
        transmissionId: '1',
        transcript: 'Hello',
        canonicalAudioUri: 'gs:://foo.flac',
        startTimestamp: '2026-04-10T12:00:00Z',
      },
    ];
    vi.mocked(listTranscripts).mockResolvedValueOnce(mockTranscripts);

    render(<TranscriptView />);

    const input = screen.getByLabelText(/Enter Feed ID/i);
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Hello')).toBeTruthy();
      expect(screen.getByLabelText('play')).toBeTruthy();
    });
  });

  it('shows error message on failure', async () => {
    vi.mocked(listTranscripts).mockRejectedValueOnce(new Error('Fetch failed'));

    render(<TranscriptView />);

    const input = screen.getByLabelText(/Enter Feed ID/i);
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Fetch failed')).toBeTruthy();
    });
  });
});
