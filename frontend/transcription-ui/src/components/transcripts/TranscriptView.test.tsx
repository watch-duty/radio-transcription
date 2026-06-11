// @vitest-environment jsdom
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { Howl } from 'howler';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  act,
  cleanup,
  fireEvent,
  screen,
  waitFor,
} from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
  type BackendFeedStatus,
  type FeedStatus,
  SourceType,
} from '@transcription/common';

import { consolidateAudioSegments } from '../../hooks/useConsolidatedAudioSegments';
import { getFeed } from '../../service/getFeed';
import { listAudioSegments } from '../../service/listAudioSegments';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
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

function makeMockAudioSegment(
  id: string,
  feedId: string,
  startTimestamp: string,
  endTimestamp: string,
  transcriptText: string,
  playbackAudioUri: string,
  evaluationDecisions: string[] = []
): AudioSegment {
  return {
    id,
    feedId,
    classification: AudioClassification.SPEECH,
    startTimestamp,
    endTimestamp,
    canonicalAudioUri: 'gs://bucket/audio.flac',
    playbackAudioUri,
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0s',
    endAudioOffset: '5s',
    createdAt: startTimestamp,
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: startTimestamp,
        data: {
          text: transcriptText,
          errors: [],
        },
      },
      ...(evaluationDecisions.length > 0
        ? [
            {
              type: AnnotationType.EVALUATION,
              createdAt: startTimestamp,
              data: {
                decisions: evaluationDecisions,
                errors: [],
              },
            },
          ]
        : []),
    ],
  };
}

vi.mock('../../service/listAudioSegments', () => ({
  listAudioSegments: vi.fn(),
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
    makeMockAudioSegment(
      '1',
      'feed123',
      '2026-04-10T12:00:00Z',
      '2026-04-10T12:00:05Z',
      'Hello',
      'gs:://foo.m4a',
      []
    ),
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    mockHandleError.mockClear();
    // Default mock for listTranscripts to prevent errors on mount
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [],
      nextToken: undefined,
    });
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
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [],
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Hello',
        'gs:://foo.m4a',
        []
      ),
    ];
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: mockTranscripts,
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Oldest',
        'gs:://foo.m4a',
        []
      ),
      makeMockAudioSegment(
        '3',
        'feed123',
        '2026-04-10T12:10:00Z',
        '2026-04-10T12:10:05Z',
        'Newest',
        'gs:://foo.m4a',
        []
      ),
      makeMockAudioSegment(
        '2',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'Middle',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: mockUnsortedTranscripts,
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
    vi.mocked(listAudioSegments).mockRejectedValueOnce(
      new Error('Fetch failed')
    );

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
      expect(listFeeds).toHaveBeenCalledTimes(2);
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
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [],
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
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: mockTranscripts,
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
      makeMockAudioSegment(
        'target-id',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Hello target',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: mockTranscripts,
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

  it('passes correct params to listAudioSegments when loading older transcripts', async () => {
    const initialTranscripts = [
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: 'next-token-123',
      })
      .mockResolvedValueOnce({
        segments: [],
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
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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

  it('passes correct params to listAudioSegments when loading newer transcripts', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: 'next-token-newer',
      })
      .mockResolvedValueOnce({
        segments: [],
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
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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

  it('passes correct params to listAudioSegments when loading newer transcripts with alerts filter active', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialTranscripts = [
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1 (Alert)',
        'gs:://foo.m4a',
        ['Rule A']
      ),
    ];

    const alertTranscripts = [
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 2 (Alert only)',
        'gs:://foo.m4a',
        ['Rule A']
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: alertTranscripts,
        nextToken: 'next-token-alert-newer',
      })
      .mockResolvedValueOnce({
        segments: [],
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
      expect(listAudioSegments).toHaveBeenCalledTimes(3);
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    const newerTranscripts = [
      makeMockAudioSegment(
        '2',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'Newer Transcript',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: newerTranscripts,
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
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(screen.getByText('Newer Transcript')).toBeTruthy();
    });

    vi.useRealTimers();
  });

  it('polls for feed status in background', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [],
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
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: mockTranscripts,
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    const newerTranscripts = [
      makeMockAudioSegment(
        '2',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'Newer Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: newerTranscripts,
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    const newerTranscripts = [
      makeMockAudioSegment(
        '2',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'Newer Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: newerTranscripts,
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
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    // Default load should have been triggered on mount due to feedId param
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
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
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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
      makeMockAudioSegment(
        '1',
        'feed123',
        testTimestampString,
        new Date(new Date(testTimestampString).getTime() + 5000).toISOString(),
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialTranscripts,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: [],
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
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:00:00Z',
        '2026-04-10T12:00:05Z',
        'Transcript 1',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: initialTranscripts,
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
      expect(listAudioSegments).toHaveBeenLastCalledWith(
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

  it('advances playback to the next silence segment inside a silence bundle when the current one finishes', async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let lastHowlOptions: any = null;
    const initSpy = vi
      .spyOn(Howl.prototype, 'init')
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .mockImplementation(function (this: any, o: any) {
        lastHowlOptions = o;
        this._sounds = [];
        return this;
      });

    const playSpy = vi
      .spyOn(Howl.prototype, 'play')
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .mockImplementation(function (this: any) {
        if (lastHowlOptions?.onplay) {
          lastHowlOptions.onplay();
        }
        return this;
      });

    const mockSilence1 = {
      id: 'silence-1',
      feedId: 'feed123',
      classification: AudioClassification.OTHER,
      startTimestamp: '2026-04-10T12:00:00Z',
      endTimestamp: '2026-04-10T12:00:05Z',
      playbackAudioUri: 'gs://bucket/silence-1.m4a',
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
      annotations: [],
      createdAt: '2026-04-10T12:00:00Z',
    };

    const mockSilence2 = {
      id: 'silence-2',
      feedId: 'feed123',
      classification: AudioClassification.OTHER,
      startTimestamp: '2026-04-10T12:00:05Z',
      endTimestamp: '2026-04-10T12:00:10Z',
      playbackAudioUri: 'gs://bucket/silence-2.m4a',
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
      annotations: [],
      createdAt: '2026-04-10T12:00:05Z',
    };

    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [mockSilence1, mockSilence2],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('[No speech detected]')).toBeTruthy();
    });

    const playButton = screen.getAllByLabelText('play')[1];
    fireEvent.click(playButton);

    expect(initSpy).toHaveBeenCalled();
    expect(lastHowlOptions.src).toEqual([
      expect.stringContaining('silence-1.m4a'),
    ]);
    expect(playSpy).toHaveBeenCalled();

    act(() => {
      lastHowlOptions.onend();
    });

    await waitFor(() => {
      expect(lastHowlOptions.src).toEqual([
        expect.stringContaining('silence-2.m4a'),
      ]);
    });
  });

  describe('consolidateAudioSegments', () => {
    it('correctly consolidates consecutive silence segments and sorts the newest to the top', () => {
      const speech1: AudioSegment = {
        id: 'speech-1',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00Z',
        endTimestamp: '2026-04-10T12:00:05Z',
        createdAt: '2026-04-10T12:00:00Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const silence1: AudioSegment = {
        id: 'silence-1',
        feedId: 'feed-123',
        classification: AudioClassification.OTHER,
        startTimestamp: '2026-04-10T12:00:05Z',
        endTimestamp: '2026-04-10T12:00:10Z',
        createdAt: '2026-04-10T12:00:05Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const silence2: AudioSegment = {
        id: 'silence-2',
        feedId: 'feed-123',
        classification: AudioClassification.OTHER,
        startTimestamp: '2026-04-10T12:00:10Z',
        endTimestamp: '2026-04-10T12:00:15Z',
        createdAt: '2026-04-10T12:00:10Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([speech1, silence1, silence2]);
      expect(result).toHaveLength(2);

      // Silence bundle should be at the top (newest startTimestamp)
      expect(result[0].isSilenceBundle).toBe(true);
      expect(result[0].startTimestamp).toBe('2026-04-10T12:00:05Z');
      expect(result[0].endTimestamp).toBe('2026-04-10T12:00:15Z');
      expect(result[0].bundledSegmentIds).toEqual(['silence-1', 'silence-2']);

      // Speech should be next
      expect(result[1].id).toBe('speech-1');
    });
  });
});
