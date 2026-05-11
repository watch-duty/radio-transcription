import { Fragment } from 'react';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import type { Transcript } from '@transcription/common';

import AudioPlayer from '../audio/AudioPlayer';
import AlertTooltip from './AlertTooltip';

interface TranscriptRowProps {
  transcript: Transcript;
  index: number;
  totalTranscripts: number;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  onToggleAudio: (transmissionId: string, audioUri: string) => void;
  currentlyPlayingTransmissionId: string | null;
  isPlaying: boolean;
  triggerSnackbar: (message: string) => void;
  showHeader: boolean;
  isHighlighted?: boolean;
}

export function TranscriptRow({
  transcript,
  index,
  totalTranscripts,
  ruleIdToNameMap,
  rulesLoading,
  onToggleAudio,
  currentlyPlayingTransmissionId,
  isPlaying,
  triggerSnackbar,
  showHeader,
  isHighlighted = false,
}: TranscriptRowProps) {
  const theme = useTheme();
  const currentDate = new Date(transcript.startTimestamp);

  return (
    <Fragment>
      {showHeader && (
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
            }}
          >
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ fontWeight: 'bold' }}
            >
              {currentDate.toLocaleDateString([], {
                weekday: 'long',
                month: 'long',
                day: 'numeric',
                year: 'numeric',
              })}
            </Typography>
          </Box>
        </ListItem>
      )}
      <ListItem
        id={`transcript-${transcript.transmissionId}`}
        divider={index < totalTranscripts - 1}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          py: 1.5,
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
        }}
      >
        <Box
          sx={{
            width: theme.spacing(3),
            display: 'flex',
            justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          <AlertTooltip
            evaluationDecisions={transcript.evaluationDecisions}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={rulesLoading}
          />
        </Box>
        <Box
          sx={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'flex-end',
            minWidth: 'max-content',
          }}
        >
          <Typography variant="caption" color="text.secondary">
            {currentDate.toLocaleTimeString([], {
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              timeZoneName: 'short',
              hour12: false,
            })}
          </Typography>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ opacity: 0.8 }}
          >
            {Math.round(
              (new Date(transcript.endTimestamp).getTime() -
                new Date(transcript.startTimestamp).getTime()) /
                1000
            )}{' '}
            sec
          </Typography>
        </Box>
        <AudioPlayer
          audioUri={transcript.playbackAudioUri}
          transmissionId={transcript.transmissionId}
          onToggleAudio={onToggleAudio}
          currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
          isPlaying={isPlaying}
        />
        <Typography
          variant="body1"
          sx={{ flexGrow: 1, whiteSpace: 'pre-wrap' }}
        >
          {transcript.transcript}
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexShrink: 0 }}>
          <Tooltip title="Copy transcript">
            <IconButton
              size="small"
              aria-label="copy transcript"
              onClick={() => {
                navigator.clipboard.writeText(transcript.transcript);
                triggerSnackbar('Transcript copied');
              }}
              sx={{ cursor: 'copy' }}
            >
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Copy transcript deep link">
            <IconButton
              size="small"
              aria-label="copy deeplink"
              onClick={() => {
                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', transcript.feedId);
                url.searchParams.set(
                  'transmissionId',
                  transcript.transmissionId
                );
                url.searchParams.set(
                  'timestamp',
                  new Date(transcript.startTimestamp).getTime().toString()
                );
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Transcript link copied');
              }}
              sx={{ cursor: 'copy' }}
            >
              <LinkIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </ListItem>
    </Fragment>
  );
}

export default TranscriptRow;
