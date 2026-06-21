import React from 'react';
import { GroupedVirtuoso, type VirtuosoHandle } from 'react-virtuoso';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import ListItem from '@mui/material/ListItem';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';

import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { getRelativeTimeString } from '../../utils/timeUtils';
import TranscriptRow from './TranscriptRow';

export interface TranscriptDisplayProps {
  ref?: React.Ref<VirtuosoHandle>;
  audioSegments: RenderableAudioSegment[];
  groupCounts: number[];
  groupTitles: string[];
  setIsViewAtTopOfAudioSegments: (atTop: boolean) => void;
  hasNewerAudioSegments: boolean;
  isFetchingNewerAudioSegments: boolean;
  isAudioSegmentsPolling: boolean;
  hasOlderAudioSegments: boolean;
  isFetchingOlderAudioSegments: boolean;
  fetchOlderAudioSegments: () => void;
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
  hasNewerAudioSegments,
  isFetchingNewerAudioSegments,
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
        endReached={fetchOlderAudioSegments}
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
            isFetchingNewerAudioSegments ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 1 }}>
                <CircularProgress size={28} />
              </Box>
            ) : null,
          Footer: () =>
            isFetchingOlderAudioSegments ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 1 }}>
                <CircularProgress size={28} />
              </Box>
            ) : !hasOlderAudioSegments ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
                <Typography variant="caption" color="text.secondary">
                  No more transcripts found
                </Typography>
              </Box>
            ) : null,
        }}
      />
    </Paper>
  );
};

export default TranscriptDisplay;
