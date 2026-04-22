// @vitest-environment jsdom
import React from 'react';
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

vi.mock('@wavesurfer/react', () => ({
  default: () => <div data-testid="wavesurfer-player" />,
}));

vi.mock('react-virtuoso', () => ({
  Virtuoso: ({
    data,
    itemContent,
  }: {
    data: unknown[];
    itemContent: (index: number, item: unknown) => React.ReactNode;
  }) => (
    <div data-testid="virtuoso">
      {data.map((item: unknown, index: number) => itemContent(index, item))}
    </div>
  ),
}));

describe('TranscriptView', () => {
  const mockAddAlert = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockAddAlert.mockClear();
    // Default mock for listFeeds to prevent errors on mount
    vi.mocked(listFeeds).mockResolvedValue([
      { id: 'feed123', name: 'feed123', sourceType: 'bcfy_feeds' as const },
    ]);
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
    expect(screen.getByLabelText(/Select a registered feed/i)).toBeTruthy();
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

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

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

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });

    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

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

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

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

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

    const button = screen.getByRole('button', { name: /Fetch/i });
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText('No transcripts found.')).toBeTruthy();
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

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

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

    renderWithQueryClient(
      <MemoryRouter>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    await waitFor(() => {
      expect((input as HTMLInputElement).disabled).toBe(false);
    });
    fireEvent.change(input, { target: { value: 'feed123' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

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

  it('shows source url link for searched feed when sourceUrl is available', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        sourceUrl: 'https://partner.broadcastify.com/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      const link = screen.getByText(/original source link/i);
      expect(link).toBeTruthy();
      expect(link.getAttribute('href')).toBe(
        'https://partner.broadcastify.com/12345'
      );
    });
  });

  it('shows archive url link for searched feed when archiveUrl is available', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      const link = screen.getByText(/archives/i);
      expect(link).toBeTruthy();
      expect(link.getAttribute('href')).toBe(
        'https://www.broadcastify.com/archives/feed/12345'
      );
    });
  });

  it('shows both source url and archive url links when feed has both', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        sourceUrl: 'https://partner.broadcastify.com/12345',
        archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText(/original source link/i)).toBeTruthy();
      expect(screen.getByText(/archives/i)).toBeTruthy();
    });
  });

  it('does not show source or archive url links when feed has neither', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'echo' as const,
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('No transcripts found.')).toBeTruthy();
    });

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('does not show source or archive url links before a search is performed', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        sourceUrl: 'https://partner.broadcastify.com/12345',
        archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
      },
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

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('does not show url links when searched feed id does not match any known feed', async () => {
    const mockFeeds = [
      {
        id: 'other-feed',
        name: 'Other Feed',
        sourceType: 'bcfy_feeds' as const,
        sourceUrl: 'https://partner.broadcastify.com/99999',
        archiveUrl: 'https://www.broadcastify.com/archives/feed/99999',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=unknown-feed']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('No transcripts found.')).toBeTruthy();
    });

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('source url link opens in a new tab', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        sourceUrl: 'https://partner.broadcastify.com/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      const link = screen.getByText(/original source link/i);
      expect(link.getAttribute('target')).toBe('_blank');
      expect(link.getAttribute('rel')).toBe('noopener noreferrer');
    });
  });

  it('archive url link opens in a new tab', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderWithQueryClient(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      const link = screen.getByText(/archives/i);
      expect(link.getAttribute('target')).toBe('_blank');
      expect(link.getAttribute('rel')).toBe('noopener noreferrer');
    });
  });

  it('scrolls to highlighted transcript when transmissionId is in search params', async () => {
    const mockScrollIntoView = vi.fn();
    window.HTMLElement.prototype.scrollIntoView = mockScrollIntoView;

    const mockTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: 'target-id',
        transcript: 'Hello target',
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
      <MemoryRouter
        initialEntries={['/?feedId=feed123&transmissionId=target-id']}
      >
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    // Wait for the transcript to be rendered
    await waitFor(() => {
      expect(screen.getByText('Hello target')).toBeTruthy();
    });

    // Verify scrollIntoView was called
    await waitFor(() => {
      expect(mockScrollIntoView).toHaveBeenCalled();
    });
  });
});
