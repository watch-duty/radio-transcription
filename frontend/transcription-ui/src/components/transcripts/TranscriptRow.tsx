import { Fragment } from 'react';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import {
  AudioClassification,
  type AudioSegment,
  type TranscriptAnnotationData,
} from '@transcription/common';

import {
  findEvaluationAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import AudioPlayer from '../audio/AudioPlayer';
import AlertTooltip from './AlertTooltip';

interface TranscriptRowProps {
  transcript: AudioSegment;
  index: number;
  totalTranscripts: number;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  onToggleAudio: (segmentId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  triggerSnackbar: (message: string) => void;
  showHeader: boolean;
  isHighlighted?: boolean;
  redactTranscripts?: boolean;
  onRowClick: (segmentId: string) => void;
}

export function TranscriptRow({
  transcript,
  index,
  totalTranscripts,
  ruleIdToNameMap,
  rulesLoading,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingSegmentId,
  triggerSnackbar,
  showHeader,
  isHighlighted = false,
  redactTranscripts = false,
  onRowClick,
}: TranscriptRowProps) {
  const theme = useTheme();
  const currentDate = new Date(transcript.startTimestamp);

  function renderTranscriptionText(
    transcriptAnnotation: TranscriptAnnotationData | null
  ): string {
    if (!transcriptAnnotation) {
      return 'Waiting for transcription';
    }

    if (transcriptAnnotation.errors.length > 0) {
      return 'Transcription unavailable';
    }

    return transcriptAnnotation.text;
  }

  // Only show speech-detected audio segments in the transcript list
  if (transcript.classification !== AudioClassification.SPEECH_DETECTED) {
    console.warn(
      'Skipping audio segment',
      transcript.id,
      'classification',
      transcript.classification
    );
    return null;
  }

  const evaluationAnnotation = findEvaluationAnnotationData(
    transcript.annotations
  );
  const transcriptAnnotation = findTranscriptAnnotationData(
    transcript.annotations
  );
  
  if (!transcriptAnnotation) {
    return null;
  }

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
        id={`transcript-${transcript.id}`}
        divider={index < totalTranscripts - 1}
        className="compactTable"
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
          cursor: 'pointer',
          '&:hover': {
            bgcolor: isHighlighted ? 'action.selected' : 'action.hover',
          },
        }}
        onClick={() => onRowClick(transcript.id)}
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
            evaluationDecisions={evaluationAnnotation?.decisions ?? []}
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
          audioUri={transcript.playbackAudioUri ?? ''}
          segmentId={transcript.id}
          onToggleAudio={onToggleAudio}
          isAudioPlaying={isAudioPlaying}
          currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        />
        <Typography
          variant="body1"
          sx={{
            flexGrow: 1,
            whiteSpace: 'pre-wrap',
            transition: 'filter 0.3s ease, opacity 0.3s ease',
            filter: redactTranscripts ? 'blur(6px)' : 'none',
            opacity: redactTranscripts ? 0.6 : 1,
          }}
        >
          {renderTranscriptionText(transcriptAnnotation)}
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexShrink: 0 }}>
          <Tooltip title="Copy transcript">
            <IconButton
              size="small"
              aria-label="copy transcript"
              onClick={(e) => {
                if (transcriptAnnotation?.text) {
                  e.stopPropagation();
                  navigator.clipboard.writeText(transcriptAnnotation.text);
                  triggerSnackbar('Transcript copied');
                }
              }}
              sx={{ cursor: 'copy' }}
              disabled={
                !transcriptAnnotation || transcriptAnnotation.errors.length > 0
              }
            >
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Copy transcript deep link">
            <IconButton
              size="small"
              aria-label="copy deeplink"
              onClick={(e) => {
                e.stopPropagation();
                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', transcript.feedId);
                url.searchParams.set('segmentId', transcript.id);
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
