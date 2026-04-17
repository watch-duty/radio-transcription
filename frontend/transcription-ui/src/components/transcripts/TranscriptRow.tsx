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
  onPlay: (transmissionId: string | null) => void;
  currentlyPlayingTransmissionId: string | null;
  triggerSnackbar: (message: string) => void;
  showHeader: boolean;
}

export function TranscriptRow({
  transcript,
  index,
  totalTranscripts,
  ruleIdToNameMap,
  rulesLoading,
  onPlay,
  currentlyPlayingTransmissionId,
  triggerSnackbar,
  showHeader,
}: TranscriptRowProps) {
  const theme = useTheme();
  const currentDate = new Date(transcript.startTimestamp);

  return (
    <Fragment>
      {showHeader && (
        <ListItem sx={{ py: 0.5, bgcolor: 'action.hover' }}>
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
        </ListItem>
      )}
      <ListItem
        divider={index < totalTranscripts - 1}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          py: 1.5,
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
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ minWidth: 'max-content' }}
        >
          {currentDate.toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            timeZoneName: 'short',
            hour12: false,
          })}
        </Typography>
        <AudioPlayer
          audioUri={transcript.canonicalAudioUri}
          transmissionId={transcript.transmissionId}
          onPlay={onPlay}
          currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
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
            >
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Copy link to transmission">
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
                  'startTimestamp',
                  new Date(transcript.startTimestamp).getTime().toString()
                );
                url.searchParams.set(
                  'endTimestamp',
                  new Date(transcript.endTimestamp).getTime().toString()
                );
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Link copied');
              }}
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
