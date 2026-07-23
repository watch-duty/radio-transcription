import { useState } from 'react';

import Box from '@mui/material/Box';
import ListItem from '@mui/material/ListItem';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import {
  AudioClassification,
  type TranscriptAnnotationData,
} from '@transcription/common';

import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { feedPanelWideQuery } from '../../hooks/useIsNarrow';
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
  isNarrow?: boolean;
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
  isNarrow = false,
}: TranscriptRowProps) {
  const theme = useTheme();

  const [isHovered, setIsHovered] = useState(false);

  const currentDate = new Date(audioSegment.startTimestamp);

  const isSilence = !!audioSegment.isSilenceBundle;
  const isOutage = !!audioSegment.isOutageBundle;

  const transcriptAnnotation = findTranscriptAnnotationData(
    audioSegment.annotations
  );

  const hasErrors = transcriptAnnotation
    ? transcriptAnnotation.errors.length > 0 && !transcriptAnnotation.text
    : false;
  const hasErrorsWithText = transcriptAnnotation
    ? transcriptAnnotation.errors.length > 0 && !!transcriptAnnotation.text
    : false;
  const isWaiting = !isSilence && !isOutage && !transcriptAnnotation;
  const isMissingTextButSpeech =
    !!transcriptAnnotation &&
    !transcriptAnnotation.text &&
    audioSegment.classification === AudioClassification.SPEECH &&
    !hasErrors;
  const isPlaceholder =
    isSilence || isWaiting || hasErrors || isOutage || isMissingTextButSpeech;

  const degradationReasons: string[] = [];
  if (audioSegment.missingPriorContext && audioSegment.missingPostContext) {
    degradationReasons.push(
      'Audio recording was cut off at the beginning and end'
    );
  } else if (audioSegment.missingPriorContext) {
    degradationReasons.push('Audio recording was cut off at the beginning');
  } else if (audioSegment.missingPostContext) {
    degradationReasons.push('Audio recording was cut off at the end');
  }
  if (hasErrorsWithText && transcriptAnnotation) {
    degradationReasons.push(...transcriptAnnotation.errors);
  }

  function renderTranscriptionText(
    transcriptAnnotation: TranscriptAnnotationData | null
  ): string {
    if (isOutage) {
      return '[Audio unavailable]';
    }

    if (isSilence) {
      return '[No speech detected]';
    }

    if (!transcriptAnnotation) {
      return '[Waiting on transcript]';
    }

    if (transcriptAnnotation.errors.length > 0 && !transcriptAnnotation.text) {
      return '[Transcription failed]';
    }

    if (isMissingTextButSpeech) {
      return '[Possible speech detected. No transcription available]';
    }

    return transcriptAnnotation.text;
  }

  const evaluationAnnotation = findEvaluationAnnotationData(
    audioSegment.annotations
  );
  const hasDecisions = (evaluationAnnotation?.decisions?.length ?? 0) > 0;

  // Wide (non-narrow) layout keys off the FeedPanel container's width, so rows
  // in a narrow scanner card stack even when the window is wide.
  const wide = feedPanelWideQuery(theme);

  const isCurrentlyPlaying =
    isAudioPlaying &&
    (currentlyPlayingSegmentId === audioSegment.id ||
      (audioSegment.isSilenceBundle &&
        currentlyPlayingSegmentId &&
        audioSegment.bundledSegmentIds?.includes(currentlyPlayingSegmentId)));

  const isOngoingSilence = isSilence && isTopAudioSegmentRow;

  const getBorderColor = () => {
    if (isOutage) {
      // Muted, darker grey than silence to indicate interruption
      return theme.palette.grey[400];
    }
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
          display: 'grid',
          gridTemplateColumns: '1fr auto',
          gridTemplateRows: 'auto auto',
          gridTemplateAreas: `
            "meta    actions"
            "text    text"
          `,
          alignItems: 'center',
          columnGap: 1,
          rowGap: 0.25,
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
          cursor: 'pointer',
          borderLeft: `5px solid ${getBorderColor()}`,
          pt: isSilence || isOutage ? '0px !important' : 0.75,
          pb: isSilence || isOutage ? '0px !important' : 0.75,
          px: 1.5,
          [wide]: {
            display: 'flex',
            gridTemplateColumns: 'unset',
            gridTemplateRows: 'unset',
            gridTemplateAreas: 'unset',
            columnGap: 2,
            rowGap: 2,
            px: 2,
          },
          '&:hover': {
            bgcolor: isHighlighted ? 'action.selected' : 'action.hover',
          },
        }}
        onClick={() => onRowClick(audioSegment.id)}
      >
        {/* Meta Box (Play, Alert, Time/Duration) */}
        <Box
          sx={{
            gridArea: 'meta',
            display: 'flex',
            flexDirection: 'row',
            alignItems: 'center',
            gap: 1,
            flexShrink: 0,
            width: '100%',
            [wide]: { gridArea: 'unset', gap: 2, width: 'auto' },
          }}
        >
          {/* Play Control (First on mobile, third on desktop) */}
          <Box
            sx={{
              order: 1,
              width: theme.spacing(5),
              height: theme.spacing(5),
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
              [wide]: { order: 3 },
            }}
          >
            {!isOutage && (
              <TranscriptPlayControl
                audioUri={audioSegment.playbackAudioUri ?? ''}
                segmentId={audioSegment.id}
                onToggleAudio={onToggleAudio}
                isAudioPlaying={isAudioPlaying}
                currentlyPlayingSegmentId={
                  isCurrentlyPlaying
                    ? audioSegment.id
                    : currentlyPlayingSegmentId
                }
                hideButton={isNarrow ? false : !isHovered}
              />
            )}
          </Box>

          {/* Alert Tooltip (Second on mobile, first on desktop) */}
          <Box
            sx={{
              order: 2,
              width: hasDecisions ? theme.spacing(3) : 0,
              display: hasDecisions ? 'flex' : 'none',
              justifyContent: 'center',
              flexShrink: 0,
              [wide]: { order: 1, width: theme.spacing(3), display: 'flex' },
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

          {/* Time & Duration Box (Third on mobile, second on desktop) */}
          <Box
            sx={{
              order: 3,
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              gap: 1,
              width: 'auto',
              flexShrink: 0,
              [wide]: {
                order: 2,
                flexDirection: 'column',
                alignItems: 'flex-end',
                gap: 0,
                width: 90,
              },
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
            {/* For ongoing silence at the live edge, display elapsed time without seconds
                to keep the running duration informative without causing second-by-second visual jitter during polling. */}
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{
                opacity: 0.8,
                fontStyle: isSilence || isOutage ? 'italic' : 'normal',
              }}
            >
              {formatDuration(
                (new Date(audioSegment.endTimestamp).getTime() -
                  new Date(audioSegment.startTimestamp).getTime()) /
                  1000,
                !isOngoingSilence
              )}
            </Typography>
          </Box>
        </Box>

        {/* Text Box */}
        <Box
          sx={{
            gridArea: 'text',
            flexGrow: 1,
            display: 'flex',
            alignItems: 'flex-start',
            gap: 1,
            mt: 0,
            [wide]: { gridArea: 'unset' },
          }}
        >
          <Typography
            variant={isPlaceholder ? 'caption' : 'body1'}
            color={
              hasErrors
                ? 'error'
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
              fontStyle: isPlaceholder ? 'italic' : 'normal',
            }}
          >
            {isPlaceholder ? (
              renderTranscriptionText(transcriptAnnotation)
            ) : (
              <>
                {hasErrorsWithText && (
                  <Box
                    component="span"
                    sx={{
                      display: 'block',
                      typography: 'caption',
                      fontStyle: 'italic',
                      color: 'error.main',
                      mb: 1,
                    }}
                  >
                    [Transcript may be incomplete]
                  </Box>
                )}
                <HighlightedTranscript
                  text={transcriptAnnotation?.text ?? ''}
                  ruleAnnotations={evaluationAnnotation?.ruleAnnotations}
                />
              </>
            )}
          </Typography>
        </Box>
        <Box
          sx={{
            gridArea: 'actions',
            flexShrink: 0,
            alignSelf: 'center',
            [wide]: { gridArea: 'unset' },
          }}
        >
          <SegmentInfoPopover
            audioSegment={audioSegment}
            transcriptAnnotation={transcriptAnnotation}
            isSilence={isSilence}
            isOutage={isOutage}
            hasErrors={hasErrors}
            degradationReasons={degradationReasons}
            triggerSnackbar={triggerSnackbar}
          />
        </Box>
      </ListItem>
    </Box>
  );
}

export default TranscriptRow;
