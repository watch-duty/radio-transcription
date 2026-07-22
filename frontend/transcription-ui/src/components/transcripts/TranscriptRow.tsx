import { useState } from 'react';

import { saveAs } from 'file-saver';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DonwloadIcon from '@mui/icons-material/Download';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import Popover from '@mui/material/Popover';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import {
  AudioClassification,
  type TranscriptAnnotationData,
} from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import {
  findEvaluationAnnotationData,
  findTranscriptAnnotationData,
} from '../../utils/annotationUtils';
import { getAudioUrl } from '../../utils/audioUtils';
import { formatDuration } from '../../utils/timeUtils';
import TranscriptPlayControl from '../audio/TranscriptPlayControl';
import AlertTooltip from './AlertTooltip';
import HighlightedTranscript from './HighlightedTranscript';
import { SegmentDetails } from './SegmentDetails';

// Left-aligned icon+label row for the actions inside the share popover. The
// icon is a plain child (not `startIcon`) so its `fontSize="small"` isn't
// shrunk by MUI's small-button icon sizing, keeping every popover icon equal.
const SHARE_ACTION_SX = {
  justifyContent: 'flex-start',
  textTransform: 'none',
  color: 'text.primary',
  gap: 1,
  px: 1,
} as const;

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
  const { isAdmin } = useAuth();

  const [isHovered, setIsHovered] = useState(false);
  const [shareAnchor, setShareAnchor] = useState<HTMLElement | null>(null);

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

  const closeShare = () => setShareAnchor(null);

  const handleCopyTranscript = () => {
    if (transcriptAnnotation?.text) {
      navigator.clipboard.writeText(transcriptAnnotation.text);
      triggerSnackbar('Transcript copied');
    }
    closeShare();
  };

  const handleCopyLink = () => {
    const url = new URL(window.location.origin + window.location.pathname);
    url.searchParams.set('feedId', audioSegment.feedId);
    url.searchParams.set('segmentId', audioSegment.id);
    url.searchParams.set(
      'timestamp',
      new Date(audioSegment.startTimestamp).getTime().toString()
    );
    navigator.clipboard.writeText(url.toString());
    triggerSnackbar('Transcript link copied');
    closeShare();
  };

  const handleDownloadAudio = async () => {
    if (!audioSegment.playbackAudioUri) {
      return;
    }
    try {
      const url = getAudioUrl(audioSegment.playbackAudioUri);
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Failed to fetch audio: ${response.statusText}`);
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
    closeShare();
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
            display: 'flex',
            flexDirection: 'row',
            gap: { xs: 0.5, sm: 1 },
            flexShrink: 0,
            alignSelf: 'center',
            mt: 0,
          }}
        >
          <Tooltip title="Share">
            <IconButton
              size="small"
              aria-label="Share"
              onClick={(e) => {
                e.stopPropagation();
                setShareAnchor(e.currentTarget);
              }}
            >
              <LinkIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Popover
            open={Boolean(shareAnchor)}
            anchorEl={shareAnchor}
            onClose={closeShare}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            onClick={(e) => e.stopPropagation()}
          >
            <Box
              sx={{
                p: 1,
                display: 'flex',
                flexDirection: 'column',
                gap: 0.5,
                width: isAdmin ? 240 : 180,
              }}
            >
              {!isSilence && !isOutage && (
                <Button
                  size="small"
                  onClick={handleCopyTranscript}
                  disabled={!transcriptAnnotation || hasErrors}
                  sx={SHARE_ACTION_SX}
                >
                  <ContentCopyIcon fontSize="small" />
                  Copy transcript
                </Button>
              )}
              <Button
                size="small"
                onClick={handleCopyLink}
                sx={SHARE_ACTION_SX}
              >
                <LinkIcon fontSize="small" />
                Copy link
              </Button>
              <Button
                size="small"
                onClick={handleDownloadAudio}
                disabled={!audioSegment.playbackAudioUri}
                sx={SHARE_ACTION_SX}
              >
                <DonwloadIcon fontSize="small" />
                Download audio
              </Button>
              {isAdmin && (
                <>
                  <Divider sx={{ my: 0.5 }} />
                  <Box sx={{ py: 0.5 }}>
                    <SegmentDetails
                      audioSegment={audioSegment}
                      degradationReasons={degradationReasons}
                      triggerSnackbar={triggerSnackbar}
                    />
                  </Box>
                </>
              )}
            </Box>
          </Popover>
        </Box>
      </ListItem>
    </Box>
  );
}

export default TranscriptRow;
