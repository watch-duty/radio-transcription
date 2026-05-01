// @vitest-environment jsdom
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';

import { listFeeds } from '../../service/listFeeds';
import { listTranscripts } from '../../service/listTranscripts';
import { renderWithQueryClient } from '../../test/testUtils';
import TranscriptView from './TranscriptView';

const renderTranscriptView = (ui: ReactElement) => {
  return renderWithQueryClient(
    <VirtuosoMockContext.Provider
      value={{ viewportHeight: 1000, itemHeight: 100 }}
    >
      {ui}
    </VirtuosoMockContext.Provider>
  );
};

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

describe('TranscriptView', () => {
  const mockAddAlert = vi.fn();

  const mockTranscripts = [
    {
      feedId: 'feed123',
      transmissionId: '1',
      transcript: 'Hello',
      canonicalAudioUri: 'gs:://foo.flac',
      playbackAudioUri: 'gs:://foo.m4a',
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

  beforeEach(() => {
    vi.clearAllMocks();
    mockAddAlert.mockClear();
    // Default mock for listFeeds to prevent errors on mount
    vi.mocked(listFeeds).mockResolvedValue([
      {
        id: 'feed123',
        name: 'feed123',
        sourceType: 'bcfy_feeds' as const,
        status: 'active' as const,
      },
    ]);
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('renders search field and fetch button', () => {
    renderTranscriptView(
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

    renderTranscriptView(
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
        playbackAudioUri: 'gs:://foo.m4a',
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

    renderTranscriptView(
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

    renderTranscriptView(
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
      {
        id: 'feed1',
        name: 'Feed 1',
        sourceType: 'bcfy_feeds' as const,
        status: 'active' as const,
      },
    ];
    vi.mocked(listFeeds).mockResolvedValueOnce(mockFeeds);

    renderTranscriptView(
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

    renderTranscriptView(
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

  it('shows no transcripts found message', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderTranscriptView(
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
            playbackAudioUri: 'gs:://foo.m4a',
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

    renderTranscriptView(
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
          playbackAudioUri: 'gs:://foo.m4a',
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

    renderTranscriptView(
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
  it('displays source and archive links for the active feed', async () => {
    const mockFeeds = [
      {
        id: 'feed123',
        name: 'Feed 123',
        sourceType: 'bcfy_feeds' as const,
        status: 'active' as const,
        sourceUrl: 'https://partner.broadcastify.com/12345',
        archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
      },
    ];
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: mockTranscripts,
      nextToken: undefined,
    });

    renderTranscriptView(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText(/original source link/i)).toBeTruthy();
      expect(screen.getByText(/archives/i)).toBeTruthy();
    });
  });

  it('scrolls to highlighted transcript when transmissionId is in search params', async () => {
    const mockTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: 'target-id',
        transcript: 'Hello target',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
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

    renderTranscriptView(
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
  });

  it('passes correct params to listTranscripts when loading older transcripts', async () => {
    const initialTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Transcript 1',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
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

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: initialTranscripts,
        nextToken: 'next-token-123',
      })
      .mockResolvedValueOnce({
        transcripts: [],
        nextToken: undefined,
      });

    renderTranscriptView(
      <MemoryRouter initialEntries={['/?feedId=feed123']}>
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    const loadMoreButton = screen.getByRole('button', {
      name: /Load previous transcripts/i,
    });
    fireEvent.click(loadMoreButton);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(2);
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        'fake-token',
        undefined,
        'next-token-123',
        undefined,
        undefined,
        'desc'
      );
    });
  });

  it('passes correct params to listTranscripts when loading newer transcripts', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Transcript 1',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
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

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        transcripts: [],
        nextToken: undefined,
      });

    renderTranscriptView(
      <MemoryRouter
        initialEntries={[`/?feedId=feed123&timestamp=${testTimestamp}`]}
      >
        <TranscriptView addAlert={mockAddAlert} triggerSnackbar={vi.fn()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    const loadNewerButton = screen.getByRole('button', {
      name: /Load newer transcripts/i,
    });
    fireEvent.click(loadNewerButton);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(2);
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        'fake-token',
        undefined,
        undefined,
        testTimestamp,
        undefined,
        'asc'
      );
    });
  });

  it('manual refresh button fetches newer transcripts', async () => {
    const initialTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Transcript 1',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
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

    const newerTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '2',
        transcript: 'Newer Transcript',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
        startTimestamp: '2026-04-10T12:05:00Z',
        endTimestamp: '2026-04-10T12:05:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
    ];

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        transcripts: newerTranscripts,
        nextToken: undefined,
      });

    renderTranscriptView(
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
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    const refreshButton = screen.getByRole('button', {
      name: /Refresh \(15s\)/i,
    });
    fireEvent.click(refreshButton);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(2);
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        'fake-token',
        undefined,
        undefined,
        new Date('2026-04-10T12:00:00Z').getTime(),
        undefined,
        'asc'
      );
      expect(screen.getByText('Newer Transcript')).toBeTruthy();
    });
  });

  it('polls for newer transcripts in background', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const initialTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '1',
        transcript: 'Transcript 1',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
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

    const newerTranscripts = [
      {
        feedId: 'feed123',
        transmissionId: '2',
        transcript: 'Newer Transcript',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
        startTimestamp: '2026-04-10T12:05:00Z',
        endTimestamp: '2026-04-10T12:05:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
    ];

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        transcripts: newerTranscripts,
        nextToken: undefined,
      });

    renderTranscriptView(
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
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Advance time by 15 seconds
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(2);
      expect(screen.getByText('Newer Transcript')).toBeTruthy();
    });

    vi.useRealTimers();
  });
});
