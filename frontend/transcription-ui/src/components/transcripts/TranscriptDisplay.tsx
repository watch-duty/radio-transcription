import React from 'react';
import { GroupedVirtuoso, type VirtuosoHandle } from 'react-virtuoso';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import ListItem from '@mui/material/ListItem';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import type {
  InfiniteData,
  InfiniteQueryObserverResult,
} from '@tanstack/react-query';
import type { AudioSegment } from '@transcription/common';

import type { ListAudioSegmentsData } from '../../hooks/useAudioSegments';
import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { getRelativeTimeString } from '../../utils/timeUtils';
import TranscriptRow from './TranscriptRow';

export interface TranscriptDisplayProps {
  ref?: React.Ref<VirtuosoHandle>;
  audioSegments: RenderableAudioSegment[];
  groupCounts: number[];
  groupTitles: string[];
  setIsViewAtTopOfAudioSegments: (atTop: boolean) => void;
  onScrollingChange?: (isScrolling: boolean) => void;
  hasNewerAudioSegments: boolean;
  isFetchingNewerAudioSegments: boolean;
  fetchNewerAudioSegments: () => Promise<
    InfiniteQueryObserverResult<InfiniteData<ListAudioSegmentsData>, Error>
  >;
  isAudioSegmentsFetching: boolean;
  isAudioSegmentsPolling: boolean;
  hasOlderAudioSegments: boolean;
  isFetchingOlderAudioSegments: boolean;
  fetchOlderAudioSegments: () => Promise<
    InfiniteQueryObserverResult<InfiniteData<ListAudioSegmentsData>, Error>
  >;
  // Unix timestamp in ms when the audio segments query last updated with a success.
  audioSegmentsLastUpdated: number | null;
  triggerSnackbar: (message: string) => void;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  onToggleAudio: (segmentId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  redactTranscripts: boolean;
  onRowClick: (segmentId: string) => void;
}

export const TranscriptDisplay: React.FC<TranscriptDisplayProps> = ({
  ref,
  audioSegments,
  groupCounts,
  groupTitles,
  setIsViewAtTopOfAudioSegments,
  onScrollingChange,
  hasNewerAudioSegments,
  isFetchingNewerAudioSegments,
  fetchNewerAudioSegments,
  isAudioSegmentsFetching,
  hasOlderAudioSegments,
  isFetchingOlderAudioSegments,
  fetchOlderAudioSegments,
  isAudioSegmentsPolling,
  audioSegmentsLastUpdated,
  triggerSnackbar,
  ruleIdToNameMap,
  rulesLoading,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  redactTranscripts,
  onRowClick,
}) => {
  return (
    <Paper
      variant="outlined"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      <GroupedVirtuoso
        ref={ref}
        groupCounts={groupCounts}
        atTopStateChange={(atTop) => setIsViewAtTopOfAudioSegments(atTop)}
        isScrolling={onScrollingChange}
        groupContent={(index) => {
          const title = groupTitles[index];
          return (
            <ListItem
              sx={{
                position: 'sticky',
                top: 0,
                zIndex: 1,
                py: 0,
                px: 0,
                bgcolor: 'background.paper',
              }}
            >
              <Box
                sx={{
                  width: '100%',
                  py: 0.5,
                  px: 2,
                  bgcolor: 'action.hover',
                  display: 'flex',
                  gap: 1,
                }}
              >
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ fontWeight: 'bold' }}
                >
                  {title}
                </Typography>
                {!hasNewerAudioSegments ? (
                  <>
                    {audioSegmentsLastUpdated && (
                      <Box
                        sx={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: 0.5,
                        }}
                      >
                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{ whiteSpace: 'nowrap' }}
                        >
                          Last refresh:
                        </Typography>
                        {isAudioSegmentsPolling ? (
                          <CircularProgress size={12} />
                        ) : (
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            sx={{ whiteSpace: 'nowrap' }}
                          >
                            {getRelativeTimeString(
                              audioSegmentsLastUpdated,
                              false
                            )}
                          </Typography>
                        )}
                      </Box>
                    )}
                  </>
                ) : (
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{ whiteSpace: 'nowrap' }}
                  >
                    Refresh disabled while viewing historical data
                  </Typography>
                )}
              </Box>
            </ListItem>
          );
        }}
        itemContent={(index) => {
          const audioSegment = audioSegments[index];
          return (
            <TranscriptRow
              key={audioSegment.id}
              audioSegment={audioSegment}
              index={index}
              totalAudioSegments={audioSegments.length}
              ruleIdToNameMap={ruleIdToNameMap}
              rulesLoading={rulesLoading}
              onToggleAudio={onToggleAudio}
              isAudioPlaying={isAudioPlaying}
              currentlyPlayingSegmentId={currentlyPlayingSegmentId}
              triggerSnackbar={triggerSnackbar}
              showHeader={false}
              isHighlighted={
                audioSegment.id === highlightedSegmentId ||
                (audioSegment.isSilenceBundle &&
                  audioSegment.bundledSegmentIds?.includes(
                    highlightedSegmentId ?? ''
                  ))
              }
              redactTranscripts={redactTranscripts}
              onRowClick={onRowClick}
              isTopAudioSegmentRow={index === 0 && !hasNewerAudioSegments}
            />
          );
        }}
        components={{
          Header: () =>
            hasNewerAudioSegments ? (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  py: 1,
                  gap: 1,
                }}
              >
                {isFetchingNewerAudioSegments ? (
                  <CircularProgress size={40} />
                ) : (
                  <Button
                    variant="outlined"
                    onClick={async () => {
                      const result = await fetchNewerAudioSegments();
                      if (
                        result.data &&
                        (
                          result.data.pages[0] as {
                            segments: AudioSegment[];
                          }
                        )?.segments.length === 0
                      ) {
                        triggerSnackbar('No newer transcripts found');
                      }
                    }}
                    disabled={isAudioSegmentsFetching}
                    sx={{ minWidth: '160px', textTransform: 'none' }}
                  >
                    Load newer transcripts
                  </Button>
                )}
              </Box>
            ) : null,
          Footer: () => {
            if (hasOlderAudioSegments) {
              return (
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'center',
                    py: 1,
                  }}
                >
                  {isFetchingOlderAudioSegments ? (
                    <CircularProgress size={40} />
                  ) : (
                    <Button
                      variant="outlined"
                      onClick={() => fetchOlderAudioSegments()}
                      disabled={isAudioSegmentsFetching}
                      sx={{ minWidth: '160px', textTransform: 'none' }}
                    >
                      Load older transcripts
                    </Button>
                  )}
                </Box>
              );
            }
            return (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  py: 2,
                }}
              >
                <Typography variant="caption" color="text.secondary">
                  No more transcripts found
                </Typography>
              </Box>
            );
          },
        }}
      />
    </Paper>
  );
};

export default TranscriptDisplay;
