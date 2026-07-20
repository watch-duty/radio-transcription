import { useState } from 'react';

import { saveAs } from 'file-saver';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DonwloadIcon from '@mui/icons-material/Download';
import EditIcon from '@mui/icons-material/Edit';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  AnnotationType,
  AudioClassification,
  type TranscriptAnnotationData,
} from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { createUserGeneratedTranscript } from '../../service/createUserGeneratedTranscript';
import {
  findEvaluationAnnotationData,
  findOriginalTranscriptAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import { getAudioUrl } from '../../utils/audioUtils';
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
  isMobile?: boolean;
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
  isMobile = false,
}: TranscriptRowProps) {
  const theme = useTheme();
  const { isAdmin, token } = useAuth();
  const queryClient = useQueryClient();

  const [isHovered, setIsHovered] = useState(false);
  const [isEditingTranscript, setIsEditingTranscript] = useState(false);
  const [editedTranscript, setEditedTranscript] = useState('');

  const editMutation = useMutation({
    mutationFn: (text: string) =>
      createUserGeneratedTranscript(audioSegment.id, token ?? '', text),
    onSuccess: () => {
      setIsEditingTranscript(false);
      triggerSnackbar('Manual transcript submitted');

      queryClient.invalidateQueries({ queryKey: ['listAudioSegments'] });
    },
    onError: (error) => {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      triggerSnackbar(`Failed to edit transcript: ${errorMessage}`);
    },
  });

  const currentDate = new Date(audioSegment.startTimestamp);

  const isSilence = !!audioSegment.isSilenceBundle;
  const isOutage = !!audioSegment.isOutageBundle;

  const transcriptAnnotation = findTranscriptAnnotationData(
    audioSegment.annotations
  );

  const originalTranscriptAnnotation = findOriginalTranscriptAnnotationData(
    audioSegment.annotations
  );

  const isUserGenerated = audioSegment.annotations.some(
    (a) => a.type === AnnotationType.USER_GENERATED_TRANSCRIPT
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
              "text    actions"
            `,
            sm: 'unset',
          },
          alignItems: { xs: 'stretch', sm: 'center' },
          gap: { xs: 1.5, sm: 2 },
          bgcolor: isHighlighted ? 'action.selected' : 'inherit',
          scrollMarginTop: theme.spacing(5),
          cursor: 'pointer',
          borderLeft: `5px solid ${getBorderColor()}`,
          pt:
            isSilence || isOutage
              ? '0px !important'
              : { xs: 1.5, sm: undefined },
          pb:
            isSilence || isOutage
              ? '0px !important'
              : { xs: 1.5, sm: undefined },
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
                hideButton={isMobile ? false : !isHovered}
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
            mt: { xs: 0.5, sm: 0 },
          }}
        >
          {isEditingTranscript ? (
            <Box
              sx={{
                width: '100%',
                display: 'flex',
                flexDirection: 'column',
                gap: 1,
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <TextField
                fullWidth
                multiline
                size="small"
                value={editedTranscript}
                onChange={(e) => setEditedTranscript(e.target.value)}
                autoFocus
              />
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
                <Button
                  size="small"
                  onClick={() => setIsEditingTranscript(false)}
                  disabled={editMutation.isPending}
                >
                  Cancel
                </Button>
                <Button
                  size="small"
                  variant="contained"
                  onClick={() => editMutation.mutate(editedTranscript)}
                  disabled={editMutation.isPending}
                >
                  {editMutation.isPending ? 'Saving...' : 'Save'}
                </Button>
              </Box>
            </Box>
          ) : (
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
                  {isUserGenerated && (
                    <Tooltip
                      title={
                        originalTranscriptAnnotation?.text
                          ? `Original Model Transcript: "${originalTranscriptAnnotation.text}"`
                          : 'Original transcript not available'
                      }
                      arrow
                    >
                      <Typography
                        component="span"
                        variant="caption"
                        color="text.secondary"
                        sx={{
                          ml: 1,
                          fontStyle: 'italic',
                          alignSelf: 'center',
                          cursor: 'help',
                        }}
                      >
                        (edited)
                      </Typography>
                    </Tooltip>
                  )}
                </>
              )}
            </Typography>
          )}
        </Box>
        <Box
          sx={{
            gridArea: { xs: 'actions', sm: 'unset' },
            display: 'flex',
            flexDirection: { xs: 'column', sm: 'row' },
            gap: 1,
            flexShrink: 0,
            alignSelf: { xs: 'start', sm: 'center' },
            mt: { xs: 0.5, sm: 0 },
          }}
        >
          {!isSilence && !isOutage && isAdmin && (
            <Tooltip title="Edit transcript">
              <span>
                <IconButton
                  size="small"
                  aria-label="edit transcript"
                  onClick={(e) => {
                    e.stopPropagation();
                    setEditedTranscript(transcriptAnnotation?.text ?? '');
                    setIsEditingTranscript(true);
                  }}
                >
                  <EditIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          )}
          {!isSilence && !isOutage && (
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
                  disabled={!transcriptAnnotation || hasErrors}
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
          <Tooltip title="Download audio">
            <IconButton
              size="small"
              aria-label="download audio"
              disabled={!audioSegment.playbackAudioUri}
              onClick={async (e) => {
                e.stopPropagation();

                // Button will be disabled, but need this for type safety.
                if (!audioSegment.playbackAudioUri) {
                  return;
                }

                try {
                  const url = getAudioUrl(audioSegment.playbackAudioUri);
                  const response = await fetch(url);
                  if (!response.ok) {
                    throw new Error(
                      `Failed to fetch audio: ${response.statusText}`
                    );
                  }
                  const blob = await response.blob();
                  const fileName =
                    audioSegment.playbackAudioUri.split('/').pop() ||
                    audioSegment.playbackAudioUri;
                  saveAs(blob, fileName);
                  triggerSnackbar('Audio downloaded');
                } catch (err) {
                  console.error('Failed to download audio:', err);
                  triggerSnackbar('Failed to download audio');
                }
              }}
            >
              <DonwloadIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          {isAdmin && (
            <SegmentInfoPopover
              audioSegment={audioSegment}
              degradationReasons={degradationReasons}
              triggerSnackbar={triggerSnackbar}
            />
          )}
        </Box>
      </ListItem>
    </Box>
  );
}

export default TranscriptRow;
