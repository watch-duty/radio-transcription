// @vitest-environment jsdom
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { Howl } from 'howler';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, screen, waitFor } from '@testing-library/react';
import {
  type BackendFeedStatus,
  type FeedStatus,
  SourceType,
} from '@transcription/common';

import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import { renderWithQueryClient } from '../../test/testUtils';
import TranscriptView from './TranscriptView';

const renderTranscriptView = (
  ui: ReactElement,
  options: { initialEntries?: string[] } = {}
) => {
  const { initialEntries = ['/'] } = options;
  return renderWithQueryClient(
    <MemoryRouter initialEntries={initialEntries}>
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        {ui}
      </VirtuosoMockContext.Provider>
    </MemoryRouter>
  );
};

// Mock the services
vi.mock('../../service/listTranscripts', () => ({
  listTranscripts: vi.fn(),
}));

vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

vi.mock('../../service/getFeed', () => ({
  getFeed: vi.fn(),
}));

vi.mock('../../service/listRules', () => ({
  listRules: vi.fn(),
}));

// Mock AuthContext
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-token' }),
}));

vi.mock('@wavesurfer/react', () => ({
  default: () => <div data-testid="wavesurfer-player" />,
}));

describe('TranscriptView', () => {
  const mockHandleError = vi.fn();

  const mockTranscripts = [
    {
      feedId: 'feed123',
      segmentId: '1',
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
    vi.resetAllMocks();
    mockHandleError.mockClear();
    // Default mock for listFeeds to prevent errors on mount
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: [
        {
          id: 'feed123',
          name: 'Feed 123',
          sourceType: SourceType.BCFY_FEEDS,
          status: 'active' as FeedStatus,
          substatus: 'active' as BackendFeedStatus,
        },
      ],
      total: 1,
    });
    // Default mock for getFeed
    vi.mocked(getFeed).mockResolvedValue({
      id: 'feed123',
      name: 'Feed 123',
      sourceType: SourceType.BCFY_FEEDS,
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
      lastHeartbeat: '2026-04-10T12:00:00Z',
    });
    // Default mock for listRules
    vi.mocked(listRules).mockResolvedValue([]);
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('shows loading state when fetching', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
    });
  });

  it('renders transcripts when fetched', async () => {
    const mockTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Hello')).toBeTruthy();
    });
  });

  it('sorts transcripts in descending order based on startTimestamp', async () => {
    const mockUnsortedTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
        transcript: 'Oldest',
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
      {
        feedId: 'feed123',
        segmentId: '3',
        transcript: 'Newest',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
        startTimestamp: '2026-04-10T12:10:00Z',
        endTimestamp: '2026-04-10T12:10:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: [],
      },
      {
        feedId: 'feed123',
        segmentId: '2',
        transcript: 'Middle',
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

    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: mockUnsortedTranscripts,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Newest')).toBeTruthy();
      expect(screen.getByText('Middle')).toBeTruthy();
      expect(screen.getByText('Oldest')).toBeTruthy();
    });

    const newestElement = screen
      .getByText('Newest')
      .closest('[id^="transcript-"]');
    const middleElement = screen
      .getByText('Middle')
      .closest('[id^="transcript-"]');
    const oldestElement = screen
      .getByText('Oldest')
      .closest('[id^="transcript-"]');

    expect(newestElement).toBeTruthy();
    expect(middleElement).toBeTruthy();
    expect(oldestElement).toBeTruthy();

    expect(newestElement!.compareDocumentPosition(middleElement!)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
    expect(middleElement!.compareDocumentPosition(oldestElement!)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
  });

  it('shows error message on failure', async () => {
    vi.mocked(listTranscripts).mockRejectedValueOnce(new Error('Fetch failed'));

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Error loading transcripts')).toBeTruthy();
    });
  });

  it('loads feeds on mount', async () => {
    vi.mocked(listFeeds).mockResolvedValueOnce({
      feeds: [
        {
          id: 'feed1',
          name: 'Feed 1',
          sourceType: SourceType.BCFY_FEEDS,
          status: 'active' as FeedStatus,
          substatus: 'active' as BackendFeedStatus,
        },
      ],
      total: 1,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(listFeeds).toHaveBeenCalledTimes(1);
    });
  });

  it('shows error alert when feeds fail to load', async () => {
    vi.mocked(listFeeds).mockRejectedValueOnce(new Error('Feeds load failed'));

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(mockHandleError).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'Feeds load failed' }),
        'Loading Feeds'
      );
    });
  });

  it('shows no transcripts found message', async () => {
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('No transcripts found')).toBeTruthy();
    });
  });

  it('displays source and archive links for the active feed', async () => {
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: [
        {
          id: 'feed123',
          name: 'Feed 123',
          sourceType: SourceType.BCFY_FEEDS,
          status: 'active' as FeedStatus,
          substatus: 'active' as BackendFeedStatus,
          sourceUrl: 'https://partner.broadcastify.com/12345',
          archiveUrl: 'https://www.broadcastify.com/archives/feed/12345',
        },
      ],
      total: 1,
    });
    vi.mocked(listTranscripts).mockResolvedValueOnce({
      transcripts: mockTranscripts,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText(/original source link/i)).toBeTruthy();
      expect(screen.getByText(/archives/i)).toBeTruthy();
    });
  });

  it('scrolls to highlighted transcript when segmentId is in search params', async () => {
    const mockTranscripts = [
      {
        feedId: 'feed123',
        segmentId: 'target-id',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123&segmentId=target-id'] }
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
        segmentId: '1',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: 'Feed 123' })
      ).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    const loadMoreButton = screen.getByRole('button', {
      name: /Load older transcripts/i,
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
        'desc',
        undefined
      );
    });
  });

  it('passes correct params to listTranscripts when loading newer transcripts', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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
        nextToken: 'next-token-newer',
      })
      .mockResolvedValueOnce({
        transcripts: [],
        nextToken: undefined,
      });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
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
        'next-token-newer',
        undefined,
        undefined,
        'asc',
        undefined
      );
    });
  });

  it('passes correct params to listTranscripts when loading newer transcripts with alerts filter active', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
        transcript: 'Transcript 1 (Alert)',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
        startTimestamp: '2026-04-10T12:00:00Z',
        endTimestamp: '2026-04-10T12:00:05Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs:://foo.flac'],
        startAudioOffset: '0s',
        endAudioOffset: '5s',
        evaluationDecisions: ['Rule A'],
      },
    ];

    const alertTranscripts = [
      {
        ...initialTranscripts[0],
        transcript: 'Transcript 2 (Alert only)',
      },
    ];

    vi.mocked(listTranscripts)
      .mockResolvedValueOnce({
        transcripts: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        transcripts: alertTranscripts,
        nextToken: 'next-token-alert-newer',
      })
      .mockResolvedValueOnce({
        transcripts: [],
        nextToken: undefined,
      });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1 (Alert)')).toBeTruthy();
    });

    // Open the filter menu popover
    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    // Click on the alerts filter select dropdown trigger
    const selectTrigger = screen.getByRole('combobox', { name: /Show/i });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // Click the "Apply" button to apply changes
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    // Wait for the query containing isAlert=true to complete and render
    await waitFor(() => {
      expect(screen.getByText('Transcript 2 (Alert only)')).toBeTruthy();
    });

    const loadNewerButton = screen.getByRole('button', {
      name: /Load newer transcripts/i,
    });
    fireEvent.click(loadNewerButton);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(3);
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        'fake-token',
        undefined,
        'next-token-alert-newer',
        undefined,
        undefined,
        'asc',
        true
      );
    });
  });

  it('polls for newer transcripts in background', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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
        segmentId: '2',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: 'Feed 123' })
      ).toBeInTheDocument();
    });

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

  it('polls for feed status in background', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    vi.mocked(listTranscripts).mockResolvedValue({
      transcripts: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: 'Feed 123' })
      ).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(getFeed).toHaveBeenCalledTimes(1);
    });

    // Advance time by 15 seconds
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(getFeed).toHaveBeenCalledTimes(2);
    });

    vi.useRealTimers();
  });

  it('displays the feed outlined status Chip and human-friendly relative time string', async () => {
    const fixedNow = new Date('2026-04-10T12:05:00Z');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(fixedNow);

    const mockFeed = {
      id: 'feed123',
      name: 'Feed 123',
      sourceType: SourceType.BCFY_FEEDS,
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
      lastHeartbeat: new Date(fixedNow.getTime() - 5 * 60 * 1000).toISOString(),
    };
    vi.mocked(getFeed).mockResolvedValue(mockFeed);
    vi.mocked(listTranscripts).mockResolvedValue({
      transcripts: mockTranscripts,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: 'Feed 123' })
      ).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(screen.getByText('Active')).toBeTruthy();
      expect(screen.getByText('Last updated: 5 minutes ago')).toBeTruthy();
    });

    vi.useRealTimers();
  });

  it('automatically plays newly received audio when Always play latest audio checkbox is checked', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const playSpy = vi.spyOn(Howl.prototype, 'play');

    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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
        segmentId: '2',
        transcript: 'Newer Transcript 1',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: 'Feed 123' })
      ).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Advance time by 15 seconds to trigger background polling
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(screen.getByText('Newer Transcript 1')).toBeTruthy();
    });

    // Verify that audio was automatically played since "Always play latest audio" is checked by default
    expect(playSpy).toHaveBeenCalled();

    vi.useRealTimers();
  });

  it('does not automatically play newly received audio when Always play latest audio checkbox is unchecked', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const playSpy = vi.spyOn(Howl.prototype, 'play');

    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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
        segmentId: '2',
        transcript: 'Newer Transcript 1',
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Uncheck "Always play latest audio" checkbox
    const autoplayCheckbox = screen.getByLabelText(/Always play latest audio/i);
    fireEvent.click(autoplayCheckbox);

    // Advance time by 15 seconds to trigger background polling
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(screen.getByText('Newer Transcript 1')).toBeTruthy();
    });

    // Verify that audio was NOT automatically played when disabled
    expect(playSpy).not.toHaveBeenCalled();

    vi.useRealTimers();
  });

  it('applies the isAlert filter when selected in the dropdown', async () => {
    vi.mocked(listTranscripts).mockResolvedValue({
      transcripts: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    // Default load should have been triggered on mount due to feedId param
    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledWith(
        'feed123',
        expect.any(String),
        undefined,
        undefined,
        undefined,
        undefined,
        'desc',
        undefined
      );
    });

    // Open the filter menu popover
    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    // Click on the alerts filter select dropdown trigger
    const selectTrigger = screen.getByRole('combobox', { name: /Show/i });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // Click the "Apply" button to apply changes
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    // React query should refetch transcripts using the isAlert filter
    await waitFor(() => {
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        expect.any(String),
        undefined,
        undefined,
        undefined,
        undefined,
        'desc',
        true
      );
    });
  });

  it('does not set timestamp in query/params when toggling alerts filter', async () => {
    const testTimestampString = '2026-04-10T12:00:00Z';
    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
        transcript: 'Transcript 1',
        canonicalAudioUri: 'gs:://foo.flac',
        playbackAudioUri: 'gs:://foo.m4a',
        startTimestamp: testTimestampString,
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
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Open the filter menu popover
    const filterButton = screen.getByRole('button', { name: 'filter' });
    fireEvent.click(filterButton);

    // Click on the alerts filter select dropdown trigger
    const selectTrigger = screen.getByRole('combobox', { name: /Show/i });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // Click the "Apply" button to apply changes
    const applyButton = screen.getByRole('button', { name: 'Apply' });
    fireEvent.click(applyButton);

    // React query should refetch transcripts using the isAlert filter and undefined for timestamps
    await waitFor(() => {
      expect(listTranscripts).toHaveBeenCalledTimes(2);
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        expect.any(String),
        undefined,
        undefined,
        undefined,
        undefined,
        'desc',
        true
      );
    });
  });

  it('clears the timestamp filter when clicking Jump to live', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      {
        feedId: 'feed123',
        segmentId: '1',
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

    vi.mocked(listTranscripts).mockResolvedValue({
      transcripts: initialTranscripts,
      nextToken: 'next-token-newer',
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    const jumpToLiveButton = screen.getByRole('button', {
      name: /Jump to live/i,
    });
    expect(jumpToLiveButton).not.toBeDisabled();

    fireEvent.click(jumpToLiveButton);

    await waitFor(() => {
      expect(listTranscripts).toHaveBeenLastCalledWith(
        'feed123',
        'fake-token',
        undefined,
        undefined,
        undefined,
        undefined,
        'desc',
        undefined
      );
    });
  });
});
