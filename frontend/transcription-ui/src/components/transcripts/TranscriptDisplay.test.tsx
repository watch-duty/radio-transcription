// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import { findTranscriptAnnotationData } from '../../utils/annotationUtils';
import { TranscriptDisplay } from './TranscriptDisplay';

vi.mock('./TranscriptRow', () => ({
  default: (props: { audioSegment: AudioSegment; index: number }) => {
    const transcriptData = findTranscriptAnnotationData(
      props.audioSegment.annotations
    );
    return (
      <div data-testid={`transcript-row-${props.index}`}>
        {transcriptData?.text ?? ''}
      </div>
    );
  },
}));

describe('TranscriptDisplay', () => {
  afterEach(() => {
    cleanup();
  });

  const mockAudioSegments: AudioSegment[] = [
    {
      id: 'tx-1',
      feedId: 'feed123',
      classification: AudioClassification.SPEECH,
      startTimestamp: '2026-04-10T12:00:00Z',
      endTimestamp: '2026-04-10T12:00:05Z',
      canonicalAudioUri: 'gs://audio.flac',
      playbackAudioUri: 'gs://audio.m4a',
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
      startAudioOffset: '0s',
      endAudioOffset: '5s',
      createdAt: '2026-04-10T12:00:00Z',
      annotations: [
        {
          type: AnnotationType.TRANSCRIPT,
          createdAt: '2026-04-10T12:00:00Z',
          data: {
            text: 'Hello from component first',
            errors: [],
          },
        },
      ],
    },
  ];

  it('renders virtualization list rows with correct dynamic metrics', () => {
    const mockSetTop = vi.fn();

    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={mockSetTop}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByText('Friday, April 10, 2026')).toBeTruthy();
    expect(screen.getByTestId('transcript-row-0')).toBeTruthy();
    expect(screen.getByText('Hello from component first')).toBeTruthy();
  });

  it('renders a spinner while fetching newer audio segments', () => {
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={true}
            isFetchingNewerAudioSegments={true}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('renders a spinner while fetching older audio segments', () => {
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={true}
            isFetchingOlderAudioSegments={true}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('shows no more transcripts found message when reached historical endpoint', () => {
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByText('No more transcripts found')).toBeTruthy();
  });

  it('renders "Last refresh" time with correct relative format when hasNewerTranscripts is false and transcriptsLastUpdated is provided', () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const fixedNow = new Date('2026-04-10T12:05:00Z');
    vi.setSystemTime(fixedNow);

    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={testTimestamp}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByText(/Last refresh:/i)).toBeTruthy();
    expect(screen.getByText('5 minutes ago')).toBeTruthy();

    vi.useRealTimers();
  });

  it('renders a loader/spinner instead of relative time when isTranscriptsPolling is true', () => {
    const testTimestamp = new Date('2026-04-10T12:00:00Z').getTime();
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={true}
            audioSegmentsLastUpdated={testTimestamp}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getByText(/Last refresh:/i)).toBeTruthy();
    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('does not render "Last refresh" when transcriptsLastUpdated is null', () => {
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={false}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(screen.queryByText(/Last refresh:/i)).toBeNull();
  });

  it('renders "Refresh disabled while viewing historical data" when hasNewerTranscripts is true', () => {
    render(
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <MemoryRouter>
          <TranscriptDisplay
            audioSegments={mockAudioSegments}
            firstItemIndex={0}
            groupCounts={[1]}
            groupTitles={['Friday, April 10, 2026']}
            setIsViewAtTopOfAudioSegments={vi.fn()}
            hasNewerAudioSegments={true}
            isFetchingNewerAudioSegments={false}
            hasOlderAudioSegments={false}
            isFetchingOlderAudioSegments={false}
            fetchOlderAudioSegments={vi.fn()}
            triggerSnackbar={vi.fn()}
            ruleIdToNameMap={new Map()}
            rulesLoading={false}
            onToggleAudio={vi.fn()}
            isAudioPlaying={false}
            currentlyPlayingSegmentId={null}
            highlightedSegmentId={null}
            redactTranscripts={false}
            onRowClick={vi.fn()}
            isAudioSegmentsPolling={false}
            audioSegmentsLastUpdated={null}
          />
        </MemoryRouter>
      </VirtuosoMockContext.Provider>
    );

    expect(
      screen.getByText(/Refresh disabled while viewing historical data/i)
    ).toBeTruthy();
  });
});
