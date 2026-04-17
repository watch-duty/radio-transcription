// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';

import { listFeeds } from '../../service/listFeeds';
import { listTranscripts } from '../../service/listTranscripts';
import { renderWithQueryClient } from '../../test/testUtils';
import TranscriptView from './TranscriptView';

// Mock the services
vi.mock('../../service/listTranscripts', () => ({
  listTranscripts: vi.fn(),
}));

vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

// Mock AuthContext
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-token' }),
}));

describe('TranscriptView', () => {
  const mockAddAlert = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockAddAlert.mockClear();
    // Default mock for listFeeds to prevent errors on mount
    vi.mocked(listFeeds).mockResolvedValue([]);
  });

  afterEach(() => {
    cleanup();
  });

  it('renders search field and fetch button', () => {
    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );
    expect(
      screen.getByLabelText(/Select a registered feed or enter a feed ID/i)
    ).toBeTruthy();
    expect(screen.getByRole('button', { name: /Fetch/i })).toBeTruthy();
  });

  it('shows loading state when fetching', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
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
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Hello',
        canonicalAudioUri: 'gs:://foo.flac',
        startTimestamp: '2026-04-10T12:00:00Z',
        endTimestamp: '2026-04-10T12:00:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
    ];
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: mockTranscripts,
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });

    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Hello')).toBeTruthy();
    });
  });

  it('shows error message on failure', async () => {
    vi.mocked(listTranscripts).mockRejectedValueOnce(new Error('Fetch failed'));

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Error loading transcripts.')).toBeTruthy();
    });
  });

  it('loads feeds on mount', async () => {
    const mockFeeds = [
      { id: 'feed1', name: 'Feed 1', sourceType: 'bcfy_feeds' as const },
    ];
    vi.mocked(listFeeds).mockResolvedValueOnce(mockFeeds);

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(listFeeds).toHaveBeenCalledTimes(1);
    });
  });

  it('shows error alert when feeds fail to load', async () => {
    vi.mocked(listFeeds).mockRejectedValueOnce(new Error('Feeds load failed'));

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(mockAddAlert).toHaveBeenCalledWith(
        expect.objectContaining({
          severity: 'error',
          children: expect.stringContaining('Feeds load failed'),
        })
      );
    });
  });

  it('refreshes feeds when refresh button is clicked', async () => {
    const mockFeeds = [
      { id: 'feed1', name: 'Feed 1', sourceType: 'bcfy_feeds' as const },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(listFeeds).toHaveBeenCalledTimes(1);
    });

    const refreshButton = screen.getByLabelText(/refresh feeds/i);
    await waitFor(() => {
      expect((refreshButton as HTMLButtonElement).disabled).toBe(false);
    });
    fireEvent.click(refreshButton);

    await waitFor(() => {
      expect(listFeeds).toHaveBeenCalledTimes(2);
    });
  });

  it('shows no transcripts found message', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('No transcripts found.')).toBeTruthy();
    });
  });

  it('loads more transcripts when Load More is clicked', async () => {
    const mockTranscriptsPage1 = [
      {
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Hello',
        canonicalAudioUri: 'gs:://foo.flac',
        startTimestamp: '2026-04-10T12:00:00Z',
        endTimestamp: '2026-04-10T12:00:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
    ];
    const mockTranscriptsPage2 = [
      {
        feedId: 'feed123',
        transmissionId: '2',
        transcript: 'World',
        canonicalAudioUri: 'gs:://bar.flac',
        startTimestamp: '2026-04-10T12:01:00Z',
        endTimestamp: '2026-04-10T12:01:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://bar.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
    ];

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: mockTranscriptsPage1,
        nextToken: 'token123',
      })
      .mockResolvedValueOnce({
        transcripts: mockTranscriptsPage2,
        nextToken: undefined,
      });

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Hello')).toBeTruthy();
    });

    const loadMoreButton = screen.getByRole('button', { name: /Load More/i });
    expect(loadMoreButton).toBeTruthy();
    fireEvent.click(loadMoreButton);

    await waitFor(() => {
      expect(screen.getByText('World')).toBeTruthy();
      expect(screen.getByText('Hello')).toBeTruthy();
    });
  });

  it('refetches when Fetch is clicked again with the same feedId after an error', async () => {
    const mockAddAlert = vi.fn();
    vi.mocked(listTranscripts)
      .mockRejectedValueOnce(new Error('Fetch failed'))
      .mockResolvedValueOnce({
        transcripts: [
          {
            feedId: 'feed123',
            transmissionId: '1',
            transcript: 'Success after retry',
            canonicalAudioUri: 'gs:://foo.flac',
            startTimestamp: '2026-04-10T12:00:00Z',
            endTimestamp: '2026-04-10T12:00:05Z',
            missingPriorContext: false,
            missingPostContext: false,
            sourceAudioUris: ['gs:://foo.flac'],
            startAudioOffset: '0s',
            endAudioOffset: '5s',
            evaluationDecisions: [],
          },
        ],
        nextToken: undefined,
      });

    renderWithQueryClient(<TranscriptView addAlert={mockAddAlert} />);

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Error loading transcripts.')).toBeTruthy();
    });

    // Click Fetch again without changing input
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Success after retry')).toBeTruthy();
    });
  });

  it('shows loading spinner again when Fetch is clicked with the same feedId', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [
        {
          feedId: 'feed123',
          transmissionId: '1',
          transcript: 'Initial load',
          canonicalAudioUri: 'gs:://foo.flac',
          startTimestamp: '2026-04-10T12:00:00Z',
          endTimestamp: '2026-04-10T12:00:05Z',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: ['gs:://foo.flac'],
          startAudioOffset: '0s',
          endAudioOffset: '5s',
          evaluationDecisions: [],
        },
      ],
      nextToken: undefined,
    });

    renderWithQueryClient(<TranscriptView addAlert={vi.fn()} />);

    const input = screen.getByLabelText(
      /Select a registered feed or enter a feed ID/i
    );
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('Initial load')).toBeTruthy();
    });

    let resolveTranscripts: (
      value: Awaited<ReturnType<typeof listTranscripts>>
    ) => void = () => {};
    const pendingPromise = new Promise<
      Awaited<ReturnType<typeof listTranscripts>>
    >((resolve) => {
      resolveTranscripts = resolve;
    });
    vi.mocked(listTranscripts).mockReturnValueOnce(pendingPromise);

    // Click Fetch again with the same feedId
    fireEvent.click(button);

    // The loading spinner should be displayed
    await waitFor(() => {
      expect(screen.getAllByRole('progressbar').length).toBeGreaterThan(0);
    });

    // Cleanup promise to avoid unhandled rejections
    resolveTranscripts({ transcripts: [], nextToken: undefined });
  });
});
