import { Fragment } from 'react';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { type TranscriptAnnotationData } from '@transcription/common';

import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import {
  findEvaluationAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import { formatDuration } from '../../utils/timeUtils';
import AudioPlayer from '../audio/AudioPlayer';
import AlertTooltip from './AlertTooltip';

interface TranscriptRowProps {
  transcript: RenderableAudioSegment;
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

  const isSilence = !!transcript.isSilenceBundle;

  function renderTranscriptionText(
    transcriptAnnotation: TranscriptAnnotationData | null
  ): string {
    if (isSilence) {
      return 'No transcription';
    }

    if (!transcriptAnnotation) {
      return 'Waiting for transcription';
    }

    if (transcriptAnnotation.errors.length > 0) {
      return 'Transcription unavailable';
    }

    return transcriptAnnotation.text;
  }

  const transcriptAnnotation = findTranscriptAnnotationData(
    transcript.annotations
  );

  const hasErrors = transcriptAnnotation
    ? transcriptAnnotation.errors.length > 0
    : false;
  const isWaiting = !isSilence && !transcriptAnnotation;
  const isPlaceholder = isSilence || isWaiting || hasErrors;

  const evaluationAnnotation = findEvaluationAnnotationData(
    transcript.annotations
  );

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
            evaluationDecisions={
              isSilence ? [] : (evaluationAnnotation?.decisions ?? [])
            }
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
            {formatDuration(
              (new Date(transcript.endTimestamp).getTime() -
                new Date(transcript.startTimestamp).getTime()) /
                1000
            )}
          </Typography>
        </Box>
        <AudioPlayer
          audioUri={transcript.playbackAudioUri ?? ''}
          segmentId={transcript.id}
          onToggleAudio={onToggleAudio}
          isAudioPlaying={isAudioPlaying}
          currentlyPlayingSegmentId={
            currentlyPlayingSegmentId === transcript.id ||
            (transcript.isSilenceBundle &&
              currentlyPlayingSegmentId &&
              transcript.bundledSegmentIds?.includes(currentlyPlayingSegmentId))
              ? transcript.id
              : currentlyPlayingSegmentId
          }
        />
        <Typography
          variant={isSilence ? 'caption' : 'body1'}
          color={isPlaceholder ? 'text.secondary' : 'text.primary'}
          sx={{
            flexGrow: 1,
            whiteSpace: 'pre-wrap',
            transition: 'filter 0.3s ease, opacity 0.3s ease',
            filter: redactTranscripts ? 'blur(6px)' : 'none',
            opacity: redactTranscripts ? 0.6 : 1,
            fontStyle: isSilence || isWaiting ? 'italic' : 'normal',
          }}
        >
          {renderTranscriptionText(transcriptAnnotation)}
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexShrink: 0 }}>
          <Tooltip title="Copy transcript">
            <span>
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
                  isSilence ||
                  !transcriptAnnotation ||
                  transcriptAnnotation.errors.length > 0
                }
              >
                <ContentCopyIcon fontSize="small" />
              </IconButton>
            </span>
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
