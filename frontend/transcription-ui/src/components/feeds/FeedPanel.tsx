import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { type Feed } from '@transcription/common';

import { useContainerWidth } from '../../hooks/useContainerWidth';
import { useFeedView } from '../../hooks/useFeedView';
import {
  FEED_PANEL_CONTAINER,
  NarrowContext,
  feedPanelWideQuery,
  useIsNarrow,
} from '../../hooks/useIsNarrow';
import { AudioControl } from '../audio/AudioControl';
import AudioDisplay from '../audio/AudioDisplay';
import AudioSettingsButton from '../transcripts/AudioSettingsButton';
import TranscriptActionsBar from '../transcripts/TranscriptActionsBar';
import TranscriptDisplay from '../transcripts/TranscriptDisplay';

interface FeedPanelProps {
  feed: Feed | null;
  feedId: string;
  token: string;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  triggerSnackbar: (message: string) => void;
  // Controlled date filter — the feed page persists it in the URL, the scanner
  // keeps it in local state. Omit to disable date filtering.
  dateTime?: Date | null;
  onDateTimeChange?: (date: Date | null) => void;
  // Deep-link target: scroll to and highlight this segment on load.
  targetSegmentId?: string | null;
  // Gate for enabling the segments query (page: feed list loaded; scanner: true).
  isReady?: boolean;
  // Fired with the count of newly-arrived speech segments so the page can drive
  // its unread badge / document title.
  onNewSpeechSegments?: (count: number) => void;
  // Overview preload window for the mini-map; omit to skip the 24h preload.
  preloadWindowMs?: number;
}

export function FeedPanel({
  feed,
  feedId,
  token,
  ruleIdToNameMap,
  rulesLoading,
  triggerSnackbar,
  dateTime,
  onDateTimeChange,
  targetSegmentId,
  isReady,
  onNewSpeechSegments,
  preloadWindowMs,
}: FeedPanelProps) {
  const theme = useTheme();
  // Drive the narrow layout off the panel's own rendered width, not the
  // viewport — in the scanner grid each card is far narrower than the window.
  // Fall back to the viewport check until a real width lands (0 = unmeasured
  // or momentarily hidden).
  const [containerRef, containerWidth] = useContainerWidth<HTMLDivElement>();
  const viewportNarrow = useIsNarrow();
  const isNarrow =
    containerWidth != null && containerWidth > 0
      ? containerWidth < theme.breakpoints.values.sm
      : viewportNarrow;

  const {
    audioSegments,
    rawAudioSegments,
    isReady: ready,
    isAudioSegmentsInitialLoading,
    isAudioSegmentsSuccess,
    audioSegmentsError,
    hasNewerAudioSegments,
    isFetchingNewerAudioSegments,
    isAudioSegmentsPolling,
    hasOlderAudioSegments,
    isFetchingOlderAudioSegments,
    fetchOlderAudioSegments,
    audioSegmentsLastUpdated,
    virtuosoRef,
    firstItemIndex,
    groupCounts,
    groupTitles,
    handleAtTopStateChange,
    isAudioPlaying,
    playbackIntent,
    currentlyPlayingSegmentId,
    currentAudioRef,
    handleToggleAudio,
    handleTogglePlayPause,
    skipToNext,
    skipToPrevious,
    skipToNextSpeech,
    skipToPreviousSpeech,
    skipTime,
    volumeDb,
    setVolumeDb,
    pan,
    setPan,
    speed,
    setSpeed,
    reset,
    muted,
    windowEndTime,
    windowDurationMs,
    isViewingLive,
    histogramMarks,
    rangeStartMs,
    rangeEndMs,
    playbackState,
    seekTrigger,
    handleClipClick,
    handleCenterWindow,
    highlightedSegmentId,
    handleRowClick,
    redactTranscripts,
    setRedactTranscripts,
    dateTime: activeDateTime,
    handleFilterByDateTime,
    alertFilter,
    setAlertFilter,
    searchQuery,
    setSearchQuery,
    handleJumpToLive,
  } = useFeedView({
    feed,
    feedId,
    token,
    triggerSnackbar,
    dateTime,
    onDateTimeChange,
    targetSegmentId,
    isReady,
    onNewSpeechSegments,
    preloadWindowMs,
  });

  const wide = feedPanelWideQuery(theme);

  return (
    <NarrowContext.Provider value={isNarrow}>
      <Box
        ref={containerRef}
        sx={{
          // Establish a query container so descendants (transcript rows, audio
          // controls) switch layout on this panel's width, not the viewport.
          containerType: 'inline-size',
          containerName: FEED_PANEL_CONTAINER,
          width: '100%',
          textAlign: 'left',
          display: 'flex',
          flexDirection: 'column',
          flexGrow: 1,
          minHeight: 0,
        }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            mt: 0.5,
            // Space for the alert icon that hovers above the AudioDisplay.
            mb: 1.25,
            [wide]: { mt: 1, mb: 2.5 },
          }}
        >
          <AudioControl
            sx={{ flex: 1, mb: 0 }}
            isAudioPlaying={playbackIntent === 'playing'}
            onTogglePlayPause={handleTogglePlayPause}
            onSkipToNext={skipToNext}
            onSkipToPrevious={skipToPrevious}
            onFastForward={skipToNextSpeech}
            onFastRewind={skipToPreviousSpeech}
            onSkipTime={skipTime}
            disableControls={rawAudioSegments.length === 0}
            settingsButton={
              <AudioSettingsButton
                volumeDb={volumeDb}
                setVolumeDb={setVolumeDb}
                pan={pan}
                setPan={setPan}
                speed={speed}
                setSpeed={setSpeed}
                onReset={reset}
                disableControls={rawAudioSegments.length === 0}
                muted={muted}
              />
            }
          />
        </Box>

        <AudioDisplay
          audioSegments={audioSegments}
          rawAudioSegments={rawAudioSegments}
          currentlyPlayingSegmentId={currentlyPlayingSegmentId}
          highlightedSegmentId={highlightedSegmentId}
          onClipClick={handleClipClick}
          windowEndTime={windowEndTime}
          windowDurationMs={windowDurationMs}
          histogramMarks={histogramMarks}
          rangeStartMs={rangeStartMs}
          maxEnd={rangeEndMs}
          onCenterWindow={handleCenterWindow}
          isAudioPlaying={isAudioPlaying}
          playbackState={playbackState}
          currentAudioRef={currentAudioRef}
          seekTrigger={seekTrigger}
        />

        <Box
          sx={{
            flexGrow: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
          }}
        >
          <TranscriptActionsBar
            disableJumpToLive={isViewingLive}
            redactTranscripts={redactTranscripts}
            setRedactTranscripts={setRedactTranscripts}
            dateTime={activeDateTime}
            setDateTime={handleFilterByDateTime}
            alertFilter={alertFilter}
            setAlertFilter={setAlertFilter}
            onClickViewLatest={handleJumpToLive}
            searchQuery={searchQuery}
            setSearchQuery={setSearchQuery}
          />
          {audioSegments.length > 0 && ready ? (
            <TranscriptDisplay
              ref={virtuosoRef}
              audioSegments={audioSegments}
              firstItemIndex={firstItemIndex}
              groupCounts={groupCounts}
              groupTitles={groupTitles}
              setIsViewAtTopOfAudioSegments={handleAtTopStateChange}
              hasNewerAudioSegments={hasNewerAudioSegments}
              isFetchingNewerAudioSegments={isFetchingNewerAudioSegments}
              isAudioSegmentsPolling={isAudioSegmentsPolling}
              hasOlderAudioSegments={hasOlderAudioSegments}
              isFetchingOlderAudioSegments={isFetchingOlderAudioSegments}
              fetchOlderAudioSegments={fetchOlderAudioSegments}
              audioSegmentsLastUpdated={audioSegmentsLastUpdated}
              triggerSnackbar={triggerSnackbar}
              ruleIdToNameMap={ruleIdToNameMap}
              rulesLoading={rulesLoading}
              onToggleAudio={handleToggleAudio}
              isAudioPlaying={isAudioPlaying}
              currentlyPlayingSegmentId={currentlyPlayingSegmentId}
              highlightedSegmentId={highlightedSegmentId}
              redactTranscripts={redactTranscripts}
              onRowClick={handleRowClick}
              isNarrow={isNarrow}
            />
          ) : !ready || isAudioSegmentsInitialLoading ? (
            <Box
              sx={{
                display: 'flex',
                justifyContent: 'center',
                mt: theme.spacing(2),
              }}
            >
              <CircularProgress data-testid="loading-spinner" />
            </Box>
          ) : audioSegmentsError ? (
            <Typography
              color="error"
              align="center"
              sx={{ mt: theme.spacing(2) }}
            >
              Error loading transcripts
            </Typography>
          ) : isAudioSegmentsSuccess ? (
            <Box sx={{ mt: theme.spacing(2), textAlign: 'center' }}>
              <Typography color="textSecondary" align="center">
                No transcripts found
              </Typography>
            </Box>
          ) : null}
        </Box>
      </Box>
    </NarrowContext.Provider>
  );
}

export default FeedPanel;
