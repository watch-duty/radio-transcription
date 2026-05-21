import React from 'react';
import { GroupedVirtuoso, type VirtuosoHandle } from 'react-virtuoso';

import SyncIcon from '@mui/icons-material/Sync';
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
import type { Transcript } from '@transcription/common';

import { getRelativeTimeString } from '../../utils/timeUtils';
import TranscriptRow from './TranscriptRow';
import type { ListTranscriptsData } from './TranscriptView';

export interface TranscriptDisplayProps {
  ref?: React.Ref<VirtuosoHandle>;
  transcripts: Transcript[];
  groupCounts: number[];
  groupTitles: string[];
  setIsViewAtTopOfTranscripts: (atTop: boolean) => void;
  hasNewerTranscripts: boolean;
  isFetchingNewerTranscripts: boolean;
  fetchNewerTranscripts: () => Promise<
    InfiniteQueryObserverResult<InfiniteData<ListTranscriptsData>, Error>
  >;
  isTranscriptsFetching: boolean;
  isTranscriptsPolling: boolean;
  hasOlderTranscripts: boolean;
  isFetchingOlderTranscripts: boolean;
  fetchOlderTranscripts: () => Promise<
    InfiniteQueryObserverResult<InfiniteData<ListTranscriptsData>, Error>
  >;
  transcriptsDataUpdatedAt: number | null;
  triggerSnackbar: (message: string) => void;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  onToggleAudio: (transmissionId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingTransmissionId: string | null;
  highlightedTransmissionId: string | null;
  redactTranscripts: boolean;
  onRowClick: (transmissionId: string) => void;
}

export const TranscriptDisplay: React.FC<TranscriptDisplayProps> = ({
  ref,
  transcripts,
  groupCounts,
  groupTitles,
  setIsViewAtTopOfTranscripts,
  hasNewerTranscripts,
  isFetchingNewerTranscripts,
  fetchNewerTranscripts,
  isTranscriptsFetching,
  hasOlderTranscripts,
  isFetchingOlderTranscripts,
  fetchOlderTranscripts,
  isTranscriptsPolling,
  transcriptsDataUpdatedAt,
  triggerSnackbar,
  ruleIdToNameMap,
  rulesLoading,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingTransmissionId,
  highlightedTransmissionId,
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
        atTopStateChange={(atTop) => setIsViewAtTopOfTranscripts(atTop)}
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
                {!hasNewerTranscripts ? (
                  <>
                    {transcriptsDataUpdatedAt && (
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
                        {isTranscriptsPolling ? (
                          <CircularProgress size={12} />
                        ) : (
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            sx={{ whiteSpace: 'nowrap' }}
                          >
                            {getRelativeTimeString(
                              new Date(transcriptsDataUpdatedAt).toISOString(),
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
          const transcript = transcripts[index];
          return (
            <TranscriptRow
              key={transcript.transmissionId}
              transcript={transcript}
              index={index}
              totalTranscripts={transcripts.length}
              ruleIdToNameMap={ruleIdToNameMap}
              rulesLoading={rulesLoading}
              onToggleAudio={onToggleAudio}
              isAudioPlaying={isAudioPlaying}
              currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
              triggerSnackbar={triggerSnackbar}
              showHeader={false}
              isHighlighted={
                transcript.transmissionId === highlightedTransmissionId
              }
              redactTranscripts={redactTranscripts}
              onRowClick={onRowClick}
            />
          );
        }}
        components={{
          Header: () =>
            hasNewerTranscripts ? (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  py: 1,
                }}
              >
                {isFetchingNewerTranscripts ? (
                  <CircularProgress size={40} />
                ) : (
                  <Button
                    variant="outlined"
                    onClick={async () => {
                      const result = await fetchNewerTranscripts();
                      if (
                        result.data &&
                        (
                          result.data.pages[0] as {
                            transcripts: Transcript[];
                          }
                        )?.transcripts.length === 0
                      ) {
                        triggerSnackbar('No newer transcripts found');
                      }
                    }}
                    disabled={isTranscriptsFetching}
                    sx={{ minWidth: '160px', textTransform: 'none' }}
                    startIcon={<SyncIcon />}
                  >
                    Load newer transcripts
                  </Button>
                )}
              </Box>
            ) : null,
          Footer: () => {
            if (hasOlderTranscripts) {
              return (
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'center',
                    py: 1,
                  }}
                >
                  {isFetchingOlderTranscripts ? (
                    <CircularProgress size={40} />
                  ) : (
                    <Button
                      variant="outlined"
                      onClick={() => fetchOlderTranscripts()}
                      disabled={isTranscriptsFetching}
                      sx={{ minWidth: '160px', textTransform: 'none' }}
                      startIcon={<SyncIcon />}
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
