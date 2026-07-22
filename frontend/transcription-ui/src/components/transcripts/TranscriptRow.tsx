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
import {
  findEvaluationAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import { formatDuration } from '../../utils/timeUtils';
import TranscriptPlayControl from '../audio/TranscriptPlayControl';
import AlertTooltip from './AlertTooltip';
import HighlightedTranscript from './HighlightedTranscript';
import { TranscriptSharePopover } from './TranscriptSharePopover';

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
          display: { xs: 'grid', sm: 'flex' },
          gridTemplateColumns: { xs: '1fr auto', sm: 'unset' },
          gridTemplateRows: { xs: 'auto auto', sm: 'unset' },
          gridTemplateAreas: {
            xs: `
              "meta    actions"
              "text    text"
            `,
            sm: 'unset',
          },
          alignItems: 'center',
          columnGap: { xs: 1, sm: 2 },
          rowGap: { xs: 0.25, sm: 2 },
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
          cursor: 'pointer',
          borderLeft: `5px solid ${getBorderColor()}`,
          pt:
            isSilence || isOutage
              ? '0px !important'
              : { xs: 0.75, sm: undefined },
          pb:
            isSilence || isOutage
              ? '0px !important'
              : { xs: 0.75, sm: undefined },
          px: { xs: 1.5, sm: 2 },
          '&:hover': {
            bgcolor: isHighlighted ? 'action.selected' : 'action.hover',
          },
        }}
        onClick={() => onRowClick(audioSegment.id)}
      >
        {/* Meta Box (Play, Alert, Time/Duration) */}
        <Box
          sx={{
            gridArea: { xs: 'meta', sm: 'unset' },
            display: 'flex',
            flexDirection: 'row',
            alignItems: 'center',
            gap: { xs: 1, sm: 2 },
            flexShrink: 0,
            width: { xs: '100%', sm: 'auto' },
          }}
        >
          {/* Play Control (First on mobile, third on desktop) */}
          <Box
            sx={{
              order: { xs: 1, sm: 3 },
              width: theme.spacing(5),
              height: theme.spacing(5),
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
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
              order: { xs: 2, sm: 1 },
              width: {
                xs:
                  (evaluationAnnotation?.decisions?.length ?? 0) > 0
                    ? theme.spacing(3)
                    : 0,
                sm: theme.spacing(3),
              },
              display:
                (evaluationAnnotation?.decisions?.length ?? 0) > 0
                  ? 'flex'
                  : { xs: 'none', sm: 'flex' },
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

          {/* Time & Duration Box (Third on mobile, second on desktop) */}
          <Box
            sx={{
              order: { xs: 3, sm: 2 },
              display: 'flex',
              flexDirection: { xs: 'row', sm: 'column' },
              alignItems: { xs: 'center', sm: 'flex-end' },
              gap: { xs: 1, sm: 0 },
              width: { xs: 'auto', sm: 90 },
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
            gridArea: { xs: 'text', sm: 'unset' },
            flexGrow: 1,
            display: 'flex',
            alignItems: 'flex-start',
            gap: 1,
            mt: 0,
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
            gridArea: { xs: 'actions', sm: 'unset' },
            flexShrink: 0,
            alignSelf: 'center',
          }}
        >
          <TranscriptSharePopover
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
