// @vitest-environment jsdom
import type { ReactElement } from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';

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
import { VIRTUOSO_START_INDEX } from '../../hooks/useScrollAnchor';
import { getFeed } from '../../service/getFeed';
import { listAudioSegments } from '../../service/listAudioSegments';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { renderWithQueryClient } from '../../test/testUtils';
import TranscriptView from './TranscriptView';

// The VirtuosoMockContext renders every item, which makes Virtuoso fire
// endReached on mount and never fire atTopStateChange (its scroll state machine
// is inert in jsdom). Both make pagination triggers non-deterministic. We wrap
// the real GroupedVirtuoso to capture these callbacks and withhold them from the
// real component, so tests drive scroll-to-top / scroll-to-bottom explicitly.
const virtuosoCallbacks = vi.hoisted(() => ({
  atTopStateChange: undefined as ((atTop: boolean) => void) | undefined,
  endReached: undefined as ((index: number) => void) | undefined,
  // Latest firstItemIndex prop, so tests can assert scroll anchoring on prepend.
  firstItemIndex: undefined as number | undefined,
}));

vi.mock('react-virtuoso', async (importOriginal) => {
  const actual = await importOriginal<typeof import('react-virtuoso')>();
  return {
    ...actual,
    GroupedVirtuoso: ({
      atTopStateChange,
      endReached,
      ...props
    }: Record<string, unknown> & {
      atTopStateChange?: (atTop: boolean) => void;
      endReached?: (index: number) => void;
      firstItemIndex?: number;
    }) => {
      virtuosoCallbacks.atTopStateChange = atTopStateChange;
      virtuosoCallbacks.endReached = endReached;
      virtuosoCallbacks.firstItemIndex = props.firstItemIndex as
        | number
        | undefined;
      return <actual.GroupedVirtuoso {...props} />;
    },
  };
});

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

// Simulates the user scrolling away from the top of the virtualized list and
// back, which is what triggers loading newer segments via atTopStateChange.
function scrollAwayAndBackToTop() {
  if (!virtuosoCallbacks.atTopStateChange) {
    throw new Error('atTopStateChange callback not captured');
  }
  act(() => virtuosoCallbacks.atTopStateChange!(false));
  act(() => virtuosoCallbacks.atTopStateChange!(true));
}

// Simulates the user scrolling to the bottom of the list, which triggers
// loading older segments via endReached.
function scrollToBottom() {
  if (!virtuosoCallbacks.endReached) {
    throw new Error('endReached callback not captured');
  }
  act(() => virtuosoCallbacks.endReached!(0));
}

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

const audioEngineMock = vi.hoisted(() => ({
  playSpy: vi.fn(),
  lastSrc: null as string | null,
  lastSeekTime: null as number | null,
  currentTime: 0,
  lastCallbacks: null as {
    onPlay?: () => void;
    onPause?: () => void;
    onEnd?: () => void;
    onError?: () => void;
  } | null,
}));

vi.mock('../../audio/WebAudioPlayer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../audio/WebAudioPlayer')>()),
  createAudioContext: () => ({ close: () => Promise.resolve() }),
  WebAudioPlayer: class {
    resume() {}
    setVolumeDb() {}
    setPan() {}
    setSpeed() {}
    preloadNext() {}
    // Real stop() detaches listeners before the async pause event, so onPause
    // never fires — useAudioPlayback.stop() clears playback state directly.
    stop() {}
    dispose() {}
    load(
      src: string,
      callbacks: NonNullable<typeof audioEngineMock.lastCallbacks>
    ) {
      audioEngineMock.lastSrc = src;
      audioEngineMock.lastCallbacks = callbacks;
      return {
        play: () => {
          audioEngineMock.playSpy();
          callbacks.onPlay?.();
        },
        pause: () => callbacks.onPause?.(),
        stop: () => {},
        getCurrentTime: () => audioEngineMock.currentTime,
        setCurrentTime: (time: number) => {
          audioEngineMock.lastSeekTime = time;
        },
        unload: () => {},
        off: () => {},
      };
    }
  },
}));

// MUI's segmented picker inputs are unusable under JSDOM; mock the shared field
// with a button that writes a fixed draft, then Apply (in the popover) commits it.
const DATE_FILTER_PICK = new Date('2026-04-10T09:00:00Z');
vi.mock('../common/DateTimePicker', () => ({
  DateTimePicker: ({
    setDateTime,
  }: {
    setDateTime: (value: Date | null) => void;
  }) => (
    <button onClick={() => setDateTime(DATE_FILTER_PICK)}>
      picker-set-date
    </button>
  ),
}));

describe('TranscriptView', () => {
  const mockHandleError = vi.fn();

  const mockAudioSegments = [
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

  // JSDOM implements no scroll methods; react-virtuoso calls scrollBy from an
  // rAF on prepend, which otherwise throws an unhandled "not a function" error.
  beforeAll(() => {
    HTMLElement.prototype.scrollBy = () => {};
    HTMLElement.prototype.scrollTo = () => {};
    HTMLElement.prototype.scrollIntoView = () => {};
  });

  afterAll(() => {
    Reflect.deleteProperty(HTMLElement.prototype, 'scrollBy');
    Reflect.deleteProperty(HTMLElement.prototype, 'scrollTo');
    Reflect.deleteProperty(HTMLElement.prototype, 'scrollIntoView');
  });

  beforeEach(() => {
    vi.resetAllMocks();
    mockHandleError.mockClear();
    // Default mock for listAudioSegments to prevent errors on mount
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
      lastSpeechSegmentTimestamp: Date.parse('2026-04-10T12:00:00Z'),
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
    const mockAudioSegments = [
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
      segments: mockAudioSegments,
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
      segments: mockAudioSegments,
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
    const mockAudioSegments = [
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
      segments: mockAudioSegments,
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
    const initialAudioSegments = [
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
        segments: initialAudioSegments,
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

    // Infinite scroll: scrolling to the bottom triggers loading older
    scrollToBottom();

    // Limit is anything() — the active preload sets a page size these params
    // don't assert.
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        'fake-token',
        expect.anything(),
        'next-token-123',
        undefined,
        undefined,
        'desc',
        undefined,
        undefined
      );
    });
  });

  it('passes correct params to listAudioSegments when loading newer transcripts', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialAudioSegments = [
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

    // Arg-driven so the window preload's extra fetches (it pages both ways
    // around a date) don't break the scenario: the initial asc page exposes a
    // newer-page token; everything else resolves empty.
    vi.mocked(listAudioSegments).mockImplementation(
      async (_feedId, _token, _limit, nextToken, _start, _end, order) => {
        if (order === 'asc' && !nextToken) {
          return {
            segments: initialAudioSegments,
            nextToken: 'next-token-newer',
          };
        }
        return { segments: [], nextToken: undefined };
      }
    );

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Infinite scroll: scrolling away from the top and back triggers loading newer
    scrollAwayAndBackToTop();

    // Loading newer uses ascending order from the newer-page token (whether
    // triggered by the preload or the scroll-to-top). Limit is anything() — the
    // active preload sets a page size, which these params don't assert.
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        'fake-token',
        expect.anything(),
        'next-token-newer',
        undefined,
        undefined,
        'asc',
        undefined,
        undefined
      );
    });
  });

  it('holds scroll position (lowers firstItemIndex) when newer segments prepend in date mode', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialAudioSegments = [
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
    const newerAudioSegments = [
      makeMockAudioSegment(
        'n1',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'Newer 1',
        'gs:://foo.m4a',
        []
      ),
      makeMockAudioSegment(
        'n2',
        'feed123',
        '2026-04-10T12:06:00Z',
        '2026-04-10T12:06:05Z',
        'Newer 2',
        'gs:://foo.m4a',
        []
      ),
    ];

    // Arg-driven: the initial asc page exposes a newer-page token; paging asc
    // from it yields the two newer segments; everything else resolves empty.
    vi.mocked(listAudioSegments).mockImplementation(
      async (_feedId, _token, _limit, nextToken, _start, _end, order) => {
        if (order === 'asc' && !nextToken) {
          return {
            segments: initialAudioSegments,
            nextToken: 'next-token-newer',
          };
        }
        if (order === 'asc' && nextToken === 'next-token-newer') {
          return { segments: newerAudioSegments, nextToken: undefined };
        }
        return { segments: [], nextToken: undefined };
      }
    );

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Date mode → anchoring is active. The two newer segments prepend above the
    // prior head — here via the window preload (which pages newer around the
    // date); scrolling back to top would do the same. Either way, anchoring must
    // lower firstItemIndex by the prepended count so the prior view holds.
    scrollAwayAndBackToTop();

    // Longer timeout: the preload serializes older-then-newer fetches before the
    // newer page lands.
    await waitFor(
      () => {
        expect(virtuosoCallbacks.firstItemIndex).toBe(VIRTUOSO_START_INDEX - 2);
      },
      { timeout: 4000 }
    );
  });

  it('passes correct params to listAudioSegments when loading newer transcripts with alerts filter active', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialAudioSegments = [
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

    // Arg-driven: alerts-only fetches (isAlert=true) return the alert transcript
    // with a newer-page token; the unfiltered initial load returns Transcript 1;
    // preload/other fetches resolve empty.
    vi.mocked(listAudioSegments).mockImplementation(
      async (
        _feedId,
        _token,
        _limit,
        nextToken,
        _start,
        _end,
        order,
        isAlert
      ) => {
        if (isAlert && order === 'asc' && !nextToken) {
          return {
            segments: alertTranscripts,
            nextToken: 'next-token-alert-newer',
          };
        }
        if (!isAlert && order === 'asc' && !nextToken) {
          return { segments: initialAudioSegments, nextToken: undefined };
        }
        return { segments: [], nextToken: undefined };
      }
    );

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1 (Alert)')).toBeTruthy();
    });

    // Select "Alerts only" from the inline filter (applies immediately).
    const selectTrigger = screen.getByRole('combobox', {
      name: /Transcript filter/i,
    });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // Wait for the query containing isAlert=true to complete and render
    await waitFor(() => {
      expect(screen.getByText('Transcript 2 (Alert only)')).toBeTruthy();
    });

    // Infinite scroll: scrolling away from the top and back triggers loading newer
    scrollAwayAndBackToTop();

    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        'fake-token',
        expect.anything(),
        'next-token-alert-newer',
        undefined,
        undefined,
        'asc',
        true,
        undefined
      );
    });
  });

  it('polls for newer transcripts in background', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const initialAudioSegments = [
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

    const newerAudioSegments = [
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
        segments: initialAudioSegments,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: newerAudioSegments,
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
    const anchorBefore = virtuosoCallbacks.firstItemIndex;

    // Advance time by 15 seconds
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(screen.getByText('Newer Transcript')).toBeTruthy();
    });

    // Following the live edge (at top, unfiltered): a poll prepend must NOT
    // anchor — the new item surfaces at the top rather than pinning the view.
    expect(virtuosoCallbacks.firstItemIndex).toBe(anchorBefore);

    vi.useRealTimers();
  });

  it('polls for newer transcripts in background when none initially exist', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const newerTranscripts = [
      makeMockAudioSegment(
        '1',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        'New Transcript',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: [],
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
      expect(screen.getByText('No transcripts found')).toBeTruthy();
    });

    // Advance time by 15 seconds
    vi.advanceTimersByTime(15000);

    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledTimes(2);
      expect(screen.getByText('New Transcript')).toBeTruthy();
      expect(screen.queryByText('No transcripts found')).toBeNull();
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
      lastHeartbeat: fixedNow.getTime() - 10 * 60 * 1000,
      lastSpeechSegmentTimestamp: fixedNow.getTime() - 5 * 60 * 1000,
    };
    vi.mocked(getFeed).mockResolvedValue(mockFeed);
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: mockAudioSegments,
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
      expect(screen.queryByText(/heartbeat|updated/i)).toBeNull();
      expect(screen.getByText('Last activity: 5 minutes ago')).toBeTruthy();
    });

    vi.useRealTimers();
  });

  it('automatically plays newly received audio while playing at the live edge', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    const playSpy = audioEngineMock.playSpy;

    const initialAudioSegments = [
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

    const newerAudioSegments = [
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
        segments: initialAudioSegments,
        nextToken: undefined,
      })
      .mockResolvedValueOnce({
        segments: newerAudioSegments,
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

    // Simulate Transcript 1 ending naturally
    act(() => {
      audioEngineMock.lastCallbacks?.onEnd?.();
    });

    // Verify that the new segment was automatically played
    expect(playSpy).toHaveBeenCalledTimes(2);

    vi.useRealTimers();
  });

  it('does not autoplay an incoming silence segment while listening at the live edge', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    const playSpy = audioEngineMock.playSpy;

    const initialAudioSegments = [
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
    const silenceSegment = {
      ...makeMockAudioSegment(
        '2',
        'feed123',
        '2026-04-10T12:05:00Z',
        '2026-04-10T12:05:05Z',
        '',
        'gs:://silence.m4a',
        []
      ),
      classification: AudioClassification.OTHER,
    };

    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: initialAudioSegments,
        nextToken: undefined,
      })
      .mockResolvedValue({ segments: [silenceSegment], nextToken: undefined });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => expect(screen.getByText('Transcript 1')).toBeTruthy());
    // Autostart plays the initial speech segment.
    expect(playSpy).toHaveBeenCalledTimes(1);

    // The segment ends with nothing after it → idle at the live edge (listening).
    act(() => {
      audioEngineMock.lastCallbacks?.onEnd?.();
    });

    // A live poll then delivers only a silence segment.
    const callsBefore = vi.mocked(listAudioSegments).mock.calls.length;
    vi.advanceTimersByTime(15000);
    await waitFor(() =>
      expect(vi.mocked(listAudioSegments).mock.calls.length).toBeGreaterThan(
        callsBefore
      )
    );

    // Silence must not autoplay — playback stays idle (still just the autostart).
    expect(playSpy).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
  });

  it('keeps playing intent (does not pause) when the last loaded segment ends', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Autoplay is always on: playback starts and the control shows pause.
    expect(playSpy).toHaveBeenCalledTimes(1);
    expect(screen.getAllByLabelText('pause')[0]).toBeInTheDocument();

    // End the only loaded segment. With autoplay always on, playback intent
    // stays 'playing' (waiting for live audio) rather than reverting to paused.
    act(() => {
      audioEngineMock.lastCallbacks?.onEnd?.();
    });

    expect(screen.getAllByLabelText('pause')[0]).toBeInTheDocument();
  });

  it('should resume from where left off when global play is pressed after being paused mid-way', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Since play is on by default, it starts playing automatically!
    expect(playSpy).toHaveBeenCalledTimes(1);

    // The global button shows "pause"
    const pauseButton = screen.getAllByLabelText('pause')[0];
    // Click pause to pause it mid-way
    fireEvent.click(pauseButton);

    // Verify it is paused (the global button shows "play")
    const playBtnAgain = screen.getAllByLabelText('play')[0];

    // Click play again to resume
    fireEvent.click(playBtnAgain);

    // It should have called play again on the same segment
    expect(playSpy).toHaveBeenCalledTimes(2);

    // Since it was paused mid-way, the audio source should not have changed
    expect(audioEngineMock.lastSrc).toContain('gs:://foo.m4a');
  });

  it('should stop playing when advancing past the end of the last segment, and rewind from the end back into the segment', async () => {
    const mockSegment = makeMockAudioSegment(
      '1',
      'feed123',
      '2026-04-10T12:00:00Z',
      '2026-04-10T12:00:10Z',
      'Transcript 1',
      'gs:://foo.m4a'
    );

    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [mockSegment],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });

    // Play starts automatically
    expect(audioEngineMock.lastSrc).toContain('gs:://foo.m4a');

    // We mock getCurrentTime to return 8 seconds (near the end)
    audioEngineMock.currentTime = 8;

    // Click "Advance 5 seconds" button. It will overshoot the 10-second duration.
    // 8 + 5 = 13 > 10.
    // This should seek to 10 (the end) and since it is playing, it will trigger natural track end.
    const advanceButton = screen.getByLabelText('advance 5 seconds');
    fireEvent.click(advanceButton);

    // It should seek to 10
    expect(audioEngineMock.lastSeekTime).toBe(10);

    // Simulate natural end of Transcript 1 due to seeking to the end
    act(() => {
      audioEngineMock.lastCallbacks?.onEnd?.();
    });

    // It should now stop playing and set playbackIntent to paused (which shows the play icon)
    await waitFor(() => {
      expect(screen.getAllByLabelText('play')[0]).toBeInTheDocument();
    });

    // Now, clicking "Rewind 5 seconds" while stopped/unloaded at the end
    // should recognize the baseline currentTime is 10, targetTime is 5.
    // So it should reload Transcript 1 and seek to 5.
    const rewindButton = screen.getByLabelText('rewind 5 seconds');
    fireEvent.click(rewindButton);

    expect(audioEngineMock.lastSrc).toContain('gs:://foo.m4a');
    expect(audioEngineMock.lastSeekTime).toBe(5);

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
        expect.anything(),
        undefined,
        undefined,
        undefined,
        'desc',
        undefined,
        undefined
      );
    });

    // Select "Alerts only" from the inline filter (applies immediately).
    const selectTrigger = screen.getByRole('combobox', {
      name: /Transcript filter/i,
    });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // React query should refetch transcripts using the isAlert filter
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        expect.any(String),
        expect.anything(),
        undefined,
        undefined,
        undefined,
        'desc',
        true,
        undefined
      );
    });
  });

  it('does not set timestamp in query/params when toggling alerts filter', async () => {
    const testTimestampString = '2026-04-10T12:00:00Z';
    const initialAudioSegments = [
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
        segments: initialAudioSegments,
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

    // Select "Alerts only" from the inline filter (applies immediately).
    const selectTrigger = screen.getByRole('combobox', {
      name: /Transcript filter/i,
    });
    fireEvent.mouseDown(selectTrigger);

    const optionElement = await screen.findByRole('option', {
      name: /Alerts only/i,
    });
    fireEvent.click(optionElement);

    // React query should refetch transcripts using the isAlert filter and undefined for timestamps
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        expect.any(String),
        expect.anything(),
        undefined,
        undefined,
        undefined,
        'desc',
        true,
        undefined
      );
    });
  });

  it('clears the timestamp filter when clicking Jump to live', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialAudioSegments = [
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

    const liveSegments = [
      makeMockAudioSegment(
        'live-1',
        'feed123',
        '2026-04-10T18:00:00Z',
        '2026-04-10T18:00:05Z',
        'Live transcript',
        'gs:://foo.m4a',
        []
      ),
    ];

    // Arg-driven: the initial date load (asc) exposes a newer-page token so
    // "Jump to live" is enabled; the live query (desc, after jump) returns the
    // live head; other fetches resolve empty so the preload doesn't page to cap.
    vi.mocked(listAudioSegments).mockImplementation(
      async (_feedId, _token, _limit, nextToken, _start, _end, order) => {
        if (order === 'asc' && !nextToken) {
          return {
            segments: initialAudioSegments,
            nextToken: 'next-token-newer',
          };
        }
        if (order === 'desc') {
          return { segments: liveSegments, nextToken: undefined };
        }
        return { segments: [], nextToken: undefined };
      }
    );

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

    // Clearing the timestamp filter re-queries live (descending, no timestamp).
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        'fake-token',
        expect.anything(),
        undefined,
        undefined,
        undefined,
        'desc',
        undefined,
        undefined
      );
    });

    // Starting from playing (the segment auto-plays on load), Jump to live lands
    // in the idle "listening" state at the live edge — the playhead lozenge reads
    // "Listening" rather than resuming the prior clip.
    await waitFor(() => {
      expect(screen.getByText('Listening')).toBeTruthy();
    });
  });

  it('lands in listening when clicking Jump to live while paused', async () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    const initialAudioSegments = [
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

    const liveSegments = [
      makeMockAudioSegment(
        'live-1',
        'feed123',
        '2026-04-10T18:00:00Z',
        '2026-04-10T18:00:05Z',
        'Live transcript',
        'gs:://foo.m4a',
        []
      ),
    ];

    vi.mocked(listAudioSegments).mockImplementation(
      async (_feedId, _token, _limit, nextToken, _start, _end, order) => {
        if (order === 'asc' && !nextToken) {
          return {
            segments: initialAudioSegments,
            nextToken: 'next-token-newer',
          };
        }
        if (order === 'desc') {
          return { segments: liveSegments, nextToken: undefined };
        }
        return { segments: [], nextToken: undefined };
      }
    );

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: [`/?feedId=feed123&timestamp=${testTimestamp}`] }
    );

    await waitFor(() => {
      expect(screen.getByText('Transcript 1')).toBeTruthy();
    });

    // Pause the auto-started clip so we begin from the paused state (the global
    // control flipping back to "play" confirms the intent is paused).
    fireEvent.click(screen.getAllByLabelText('pause')[0]);
    await waitFor(() => {
      expect(screen.getAllByLabelText('play')[0]).toBeTruthy();
    });

    fireEvent.click(screen.getByRole('button', { name: /Jump to live/i }));

    // Even from paused, Jump to live restores the live-follow "listening" state.
    await waitFor(() => {
      expect(screen.getByText('Listening')).toBeTruthy();
    });
  });

  it('moves the window without re-grabbing playback when the mini-map is clicked (from playing)', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => expect(screen.getByText('Transcript 1')).toBeTruthy());
    // Auto-play started; at the live edge Jump to live is disabled.
    await waitFor(() => expect(playSpy).toHaveBeenCalledTimes(1));
    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).toBeDisabled();

    // jsdom reports a zero-size rect, so stub a 100px-wide strip and click at 25%.
    const strip = screen.getByLabelText('timeline overview');
    vi.spyOn(strip, 'getBoundingClientRect').mockReturnValue({
      left: 0,
      width: 100,
    } as DOMRect);
    fireEvent.click(strip, { clientX: 25 });

    // The window moved off the live edge (Jump to live re-enables) and playback
    // was stopped — the autoplay effect must not re-grab the backlog.
    await waitFor(() =>
      expect(
        screen.getByRole('button', { name: /Jump to live/i })
      ).not.toBeDisabled()
    );
    expect(playSpy).toHaveBeenCalledTimes(1);
  });

  it('moves the window without playing when the mini-map is clicked while paused', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => expect(screen.getByText('Transcript 1')).toBeTruthy());
    await waitFor(() => expect(playSpy).toHaveBeenCalledTimes(1));

    // Pause first, then navigate.
    fireEvent.click(screen.getAllByLabelText('pause')[0]);
    await waitFor(() =>
      expect(screen.getAllByLabelText('play')[0]).toBeTruthy()
    );

    const strip = screen.getByLabelText('timeline overview');
    vi.spyOn(strip, 'getBoundingClientRect').mockReturnValue({
      left: 0,
      width: 100,
    } as DOMRect);
    fireEvent.click(strip, { clientX: 25 });

    // Window moves; a paused navigation never starts playback.
    await waitFor(() =>
      expect(
        screen.getByRole('button', { name: /Jump to live/i })
      ).not.toBeDisabled()
    );
    expect(playSpy).toHaveBeenCalledTimes(1);
  });

  it('applies the search query filter when entered in the search bar', async () => {
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [],
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed123',
        expect.any(String),
        500,
        undefined,
        undefined,
        undefined,
        'desc',
        undefined,
        undefined
      );
    });

    // Type a query into the inline search field and apply it with Enter.
    const searchInput = screen.getByPlaceholderText(/Search transcripts/i);
    fireEvent.change(searchInput, { target: { value: 'dispatch' } });
    fireEvent.keyDown(searchInput, { key: 'Enter' });

    // React query should refetch transcripts using the text query filter.
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenLastCalledWith(
        'feed123',
        expect.any(String),
        500,
        undefined,
        undefined,
        undefined,
        'desc',
        undefined,
        'dispatch'
      );
    });
  });

  it('parks without re-grabbing playback when a date filter is applied (from playing)', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => expect(screen.getByText('Transcript 1')).toBeTruthy());
    // Auto-play started; at the live edge Jump to live is disabled.
    await waitFor(() => expect(playSpy).toHaveBeenCalledTimes(1));
    expect(
      screen.getByRole('button', { name: /Jump to live/i })
    ).toBeDisabled();

    // Open the calendar and apply a date — same as a mini-map navigation.
    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(await screen.findByText('picker-set-date'));
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

    // Parked: Jump to live re-enables (a date filter is now active) and playback
    // was stopped, not re-grabbed onto the refetched list.
    await waitFor(() =>
      expect(
        screen.getByRole('button', { name: /Jump to live/i })
      ).not.toBeDisabled()
    );
    expect(playSpy).toHaveBeenCalledTimes(1);
  });

  it('parks without playing when a date filter is applied while paused', async () => {
    const playSpy = audioEngineMock.playSpy;
    const initialAudioSegments = [
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
      segments: initialAudioSegments,
      nextToken: undefined,
    });

    renderTranscriptView(
      <TranscriptView onError={mockHandleError} triggerSnackbar={vi.fn()} />,
      { initialEntries: ['/?feedId=feed123'] }
    );

    await waitFor(() => expect(screen.getByText('Transcript 1')).toBeTruthy());
    await waitFor(() => expect(playSpy).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getAllByLabelText('pause')[0]);
    await waitFor(() =>
      expect(screen.getAllByLabelText('play')[0]).toBeTruthy()
    );

    fireEvent.click(screen.getByRole('button', { name: /filter by date/i }));
    fireEvent.click(await screen.findByText('picker-set-date'));
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

    await waitFor(() =>
      expect(
        screen.getByRole('button', { name: /Jump to live/i })
      ).not.toBeDisabled()
    );
    expect(playSpy).toHaveBeenCalledTimes(1);
  });

  it('advances playback to the next silence segment inside a silence bundle when the current one finishes', async () => {
    const playSpy = audioEngineMock.playSpy;

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

    // It should have started playing automatically because play is on by default
    expect(audioEngineMock.lastSrc).toContain('silence-1.m4a');
    expect(playSpy).toHaveBeenCalled();

    act(() => {
      audioEngineMock.lastCallbacks?.onEnd?.();
    });

    await waitFor(() => {
      expect(audioEngineMock.lastSrc).toContain('silence-2.m4a');
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

    it('sorts segments by startTimestamp descending even if an older segment ends later', () => {
      const olderLongSegment: AudioSegment = {
        id: 'speech-older-long',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00Z',
        endTimestamp: '2026-04-10T12:00:10Z',
        createdAt: '2026-04-10T12:00:00Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const newerShortSegment: AudioSegment = {
        id: 'speech-newer-short',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:05Z',
        endTimestamp: '2026-04-10T12:00:08Z',
        createdAt: '2026-04-10T12:00:05Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([
        olderLongSegment,
        newerShortSegment,
      ]);
      expect(result).toHaveLength(2);

      // Newer segment (starts 12:00:05) should be at the top, despite ending earlier (12:00:08 vs 12:00:10)
      expect(result[0].id).toBe('speech-newer-short');
      expect(result[1].id).toBe('speech-older-long');
    });

    it('does not consolidate UNSPECIFIED segments if they have a transcript', () => {
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

      const unspecifiedWithTranscript: AudioSegment = {
        id: 'unspecified-1',
        feedId: 'feed-123',
        classification: AudioClassification.UNSPECIFIED,
        startTimestamp: '2026-04-10T12:00:05Z',
        endTimestamp: '2026-04-10T12:00:10Z',
        createdAt: '2026-04-10T12:00:05Z',
        annotations: [
          {
            type: AnnotationType.TRANSCRIPT,
            data: {
              text: 'Hello world',
              errors: [],
            },
            createdAt: '2026-04-10T12:00:05Z',
          },
        ],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([
        speech1,
        unspecifiedWithTranscript,
      ]);
      expect(result).toHaveLength(2);
      // Neither should be a silence bundle
      expect(result[0].isSilenceBundle).toBeUndefined();
      expect(result[1].isSilenceBundle).toBeUndefined();
    });

    it('injects outage segment for continuous feed when there is a gap > 10ms and missing context flags are true', () => {
      const segment1: AudioSegment = {
        id: 'seg-1',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00.000Z',
        endTimestamp: '2026-04-10T12:00:05.000Z',
        createdAt: '2026-04-10T12:00:00.000Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: true,
        sourceAudioUris: [],
      };

      const segment2: AudioSegment = {
        id: 'seg-2',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:05.020Z', // 20ms gap
        endTimestamp: '2026-04-10T12:00:10.000Z',
        createdAt: '2026-04-10T12:00:05.000Z',
        annotations: [],
        missingPriorContext: true,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([segment1, segment2], true);
      expect(result).toHaveLength(3);
      expect(result[0].id).toBe('seg-2');
      expect(result[1].isOutageBundle).toBe(true);
      expect(result[1].startTimestamp).toBe(segment1.endTimestamp);
      expect(result[1].endTimestamp).toBe(segment2.startTimestamp);
      expect(result[2].id).toBe('seg-1');
    });

    it('does NOT inject outage segment if missing context flags are false, even if gap > 10ms', () => {
      const segment1: AudioSegment = {
        id: 'seg-1',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00.000Z',
        endTimestamp: '2026-04-10T12:00:05.000Z',
        createdAt: '2026-04-10T12:00:00.000Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const segment2: AudioSegment = {
        id: 'seg-2',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:05.020Z', // 20ms gap
        endTimestamp: '2026-04-10T12:00:10.000Z',
        createdAt: '2026-04-10T12:00:05.000Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([segment1, segment2], true);
      expect(result).toHaveLength(2);
      expect(result[0].id).toBe('seg-2');
      expect(result[1].id).toBe('seg-1');
    });

    it('does NOT inject outage segment for non-continuous feed, even if there is a gap and missing context flags are true', () => {
      const segment1: AudioSegment = {
        id: 'seg-1',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00.000Z',
        endTimestamp: '2026-04-10T12:00:05.000Z',
        createdAt: '2026-04-10T12:00:00.000Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: true,
        sourceAudioUris: [],
      };

      const segment2: AudioSegment = {
        id: 'seg-2',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:05.020Z', // 20ms gap
        endTimestamp: '2026-04-10T12:00:10.000Z',
        createdAt: '2026-04-10T12:00:05.000Z',
        annotations: [],
        missingPriorContext: true,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([segment1, segment2], false);
      expect(result).toHaveLength(2);
      expect(result[0].id).toBe('seg-2');
      expect(result[1].id).toBe('seg-1');
    });

    it('does NOT inject outage segment if the gap is <= 10ms, even if missing context flags are true', () => {
      const segment1: AudioSegment = {
        id: 'seg-1',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:00.000Z',
        endTimestamp: '2026-04-10T12:00:05.000Z',
        createdAt: '2026-04-10T12:00:00.000Z',
        annotations: [],
        missingPriorContext: false,
        missingPostContext: true,
        sourceAudioUris: [],
      };

      const segment2: AudioSegment = {
        id: 'seg-2',
        feedId: 'feed-123',
        classification: AudioClassification.SPEECH,
        startTimestamp: '2026-04-10T12:00:05.005Z', // 5ms gap
        endTimestamp: '2026-04-10T12:00:10.000Z',
        createdAt: '2026-04-10T12:00:05.000Z',
        annotations: [],
        missingPriorContext: true,
        missingPostContext: false,
        sourceAudioUris: [],
      };

      const result = consolidateAudioSegments([segment1, segment2], true);
      expect(result).toHaveLength(2);
      expect(result[0].id).toBe('seg-2');
      expect(result[1].id).toBe('seg-1');
    });
  });
});
