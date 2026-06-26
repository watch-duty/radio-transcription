import { useState } from 'react';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { type TranscriptAnnotationData } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import {
  findEvaluationAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import { formatDuration } from '../../utils/timeUtils';
import TranscriptPlayControl from '../audio/TranscriptPlayControl';
import AlertTooltip from './AlertTooltip';
import HighlightedTranscript from './HighlightedTranscript';
import { SegmentInfoPopover } from './SegmentInfoPopover';

interface TranscriptRowProps {
  audioSegment: RenderableAudioSegment;
  index: number;
  totalAudioSegments: number;
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
  isTopAudioSegmentRow?: boolean;
}

export function TranscriptRow({
  audioSegment,
  index,
  totalAudioSegments,
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
  isTopAudioSegmentRow = false,
}: TranscriptRowProps) {
  const theme = useTheme();
  const { isAdmin } = useAuth();

  const [isHovered, setIsHovered] = useState(false);

  const currentDate = new Date(audioSegment.startTimestamp);

  const isSilence = !!audioSegment.isSilenceBundle;

  function renderTranscriptionText(
    transcriptAnnotation: TranscriptAnnotationData | null
  ): string {
    if (isSilence) {
      return '[No speech detected]';
    }

    if (!transcriptAnnotation) {
      return '[Waiting on transcript]';
    }

    if (transcriptAnnotation.errors.length > 0) {
      return '[Transcription failed]';
    }

    return transcriptAnnotation.text;
  }

  const transcriptAnnotation = findTranscriptAnnotationData(
    audioSegment.annotations
  );

  const hasErrors = transcriptAnnotation
    ? transcriptAnnotation.errors.length > 0
    : false;
  const isWaiting = !isSilence && !transcriptAnnotation;
  const isPlaceholder = isSilence || isWaiting || hasErrors;

  const evaluationAnnotation = findEvaluationAnnotationData(
    audioSegment.annotations
  );

  const isCurrentlyPlaying =
    isAudioPlaying &&
    (currentlyPlayingSegmentId === audioSegment.id ||
      (audioSegment.isSilenceBundle &&
        currentlyPlayingSegmentId &&
        audioSegment.bundledSegmentIds?.includes(currentlyPlayingSegmentId)));

  const isOngoingSilence = isSilence && isTopAudioSegmentRow;

  const getBorderColor = () => {
    if (isSilence) {
      return theme.palette.grey[200];
    }
    if (isCurrentlyPlaying) {
      return theme.palette.primary.main;
    }
    return theme.palette.primary.light;
  };

  return (
    <Box
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
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
        id={`transcript-${audioSegment.id}`}
        divider={index < totalAudioSegments - 1}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
          cursor: 'pointer',
          borderLeft: `5px solid ${getBorderColor()}`,
          pt: isSilence ? '0px !important' : undefined,
          pb: isSilence ? '0px !important' : undefined,
          '&:hover': {
            bgcolor: isHighlighted ? 'action.selected' : 'action.hover',
          },
        }}
        onClick={() => onRowClick(audioSegment.id)}
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
            width: 90,
            flexShrink: 0,
          }}
        >
          {!isSilence && (
            <Typography variant="caption" color="text.secondary">
              {currentDate.toLocaleTimeString([], {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                timeZoneName: 'short',
                hour12: false,
              })}
            </Typography>
          )}
          {!isOngoingSilence && (
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{
                opacity: 0.8,
                fontStyle: isSilence ? 'italic' : 'normal',
              }}
            >
              {formatDuration(
                (new Date(audioSegment.endTimestamp).getTime() -
                  new Date(audioSegment.startTimestamp).getTime()) /
                  1000
              )}
            </Typography>
          )}
        </Box>
        <Box
          sx={{
            width: theme.spacing(5),
            height: theme.spacing(5),
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          <TranscriptPlayControl
            audioUri={audioSegment.playbackAudioUri ?? ''}
            segmentId={audioSegment.id}
            onToggleAudio={onToggleAudio}
            isAudioPlaying={isAudioPlaying}
            currentlyPlayingSegmentId={
              isCurrentlyPlaying ? audioSegment.id : currentlyPlayingSegmentId
            }
            hideButton={!isHovered}
          />
        </Box>
        <Typography
          variant={isSilence ? 'caption' : 'body1'}
          color={
            hasErrors
              ? 'error.main'
              : isPlaceholder
                ? 'text.secondary'
                : 'text.primary'
          }
          sx={{
            flexGrow: 1,
            whiteSpace: 'pre-wrap',
            transition: 'filter 0.3s ease, opacity 0.3s ease',
            filter: redactTranscripts ? 'blur(6px)' : 'none',
            opacity: redactTranscripts ? 0.6 : 1,
            fontStyle:
              isSilence || isWaiting || hasErrors ? 'italic' : 'normal',
          }}
        >
          {/* Placeholder strings (silence/waiting/error) have no real transcript to highlight. */}
          {isPlaceholder ? (
            renderTranscriptionText(transcriptAnnotation)
          ) : (
            <HighlightedTranscript
              text={transcriptAnnotation?.text ?? ''}
              ruleAnnotations={evaluationAnnotation?.ruleAnnotations}
            />
          )}
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexShrink: 0 }}>
          {!isSilence && (
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
                    !transcriptAnnotation ||
                    transcriptAnnotation.errors.length > 0
                  }
                >
                  <ContentCopyIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          )}
          <Tooltip title="Copy transcript deep link">
            <IconButton
              size="small"
              aria-label="copy deeplink"
              onClick={(e) => {
                e.stopPropagation();
                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', audioSegment.feedId);
                url.searchParams.set('segmentId', audioSegment.id);
                url.searchParams.set(
                  'timestamp',
                  new Date(audioSegment.startTimestamp).getTime().toString()
                );
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Transcript link copied');
              }}
              sx={{ cursor: 'copy' }}
            >
              <LinkIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          {isAdmin && (
            <SegmentInfoPopover
              audioSegment={audioSegment}
              triggerSnackbar={triggerSnackbar}
            />
          )}
        </Box>
      </ListItem>
    </Box>
  );
}

export default TranscriptRow;
