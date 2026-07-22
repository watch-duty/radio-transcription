import { useState } from 'react';

import { saveAs } from 'file-saver';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DownloadIcon from '@mui/icons-material/Download';
import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import Popover from '@mui/material/Popover';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  AudioSegment,
  TranscriptAnnotationData,
} from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { getAudioUrl } from '../../utils/audioUtils';

// Icon is a plain child (not `startIcon`) so its `fontSize="small"` isn't
// shrunk by MUI's small-button icon sizing.
const SHARE_ACTION_SX = {
  justifyContent: 'flex-start',
  textTransform: 'none',
  color: 'text.primary',
  gap: 1,
  px: 1,
} as const;

interface SegmentInfoPopoverProps {
  audioSegment: AudioSegment;
  transcriptAnnotation: TranscriptAnnotationData | null;
  isSilence: boolean;
  isOutage: boolean;
  hasErrors: boolean;
  degradationReasons: string[];
  triggerSnackbar: (message: string) => void;
}

// Per-row "Share" popover: copy transcript / copy link / download audio, plus
// inline segment details for admins.
export function SegmentInfoPopover({
  audioSegment,
  transcriptAnnotation,
  isSilence,
  isOutage,
  hasErrors,
  degradationReasons,
  triggerSnackbar,
}: SegmentInfoPopoverProps) {
  const { isAdmin } = useAuth();
  const { id, externalAudioSegmentId } = audioSegment;
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const close = () => setAnchor(null);

  const handleCopyTranscript = () => {
    if (transcriptAnnotation?.text) {
      navigator.clipboard.writeText(transcriptAnnotation.text);
      triggerSnackbar('Transcript copied');
    }
    close();
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
    close();
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
    close();
  };

  return (
    <>
      <Tooltip title="Share">
        <IconButton
          size="small"
          aria-label="Share"
          aria-haspopup="dialog"
          aria-expanded={Boolean(anchor)}
          onClick={(e) => {
            e.stopPropagation();
            setAnchor(e.currentTarget);
          }}
        >
          <LinkIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Popover
        open={Boolean(anchor)}
        anchorEl={anchor}
        onClose={close}
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
          <Button size="small" onClick={handleCopyLink} sx={SHARE_ACTION_SX}>
            <LinkIcon fontSize="small" />
            Copy link
          </Button>
          <Button
            size="small"
            onClick={handleDownloadAudio}
            disabled={!audioSegment.playbackAudioUri}
            sx={SHARE_ACTION_SX}
          >
            <DownloadIcon fontSize="small" />
            Download audio
          </Button>
          {isAdmin && (
            <>
              <Divider sx={{ my: 0.5 }} />
              <Box
                sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 1,
                  py: 0.5,
                }}
              >
                <Box
                  sx={{ display: 'flex', alignItems: 'center', gap: 1, pl: 1 }}
                >
                  <Tooltip title="Copy Segment ID">
                    <IconButton
                      size="small"
                      aria-label="copy segment id"
                      onClick={(e) => {
                        e.stopPropagation();
                        navigator.clipboard.writeText(id);
                        triggerSnackbar('Segment ID copied');
                      }}
                      sx={{ p: 0 }}
                    >
                      <ContentCopyIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Box sx={{ minWidth: 0 }}>
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ display: 'block' }}
                    >
                      Segment ID
                    </Typography>
                    <Typography
                      variant="body2"
                      sx={{
                        fontFamily: 'monospace',
                        fontSize: '0.65rem',
                        wordBreak: 'break-all',
                      }}
                    >
                      {id}
                    </Typography>
                  </Box>
                </Box>

                {externalAudioSegmentId && (
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                      pl: 1,
                    }}
                  >
                    <Tooltip title="Copy External ID">
                      <IconButton
                        size="small"
                        aria-label="copy external segment id"
                        onClick={(e) => {
                          e.stopPropagation();
                          navigator.clipboard.writeText(externalAudioSegmentId);
                          triggerSnackbar('External segment ID copied');
                        }}
                        sx={{ p: 0 }}
                      >
                        <ContentCopyIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Box sx={{ minWidth: 0 }}>
                      <Typography
                        variant="caption"
                        color="text.secondary"
                        sx={{ display: 'block' }}
                      >
                        External ID
                      </Typography>
                      <Typography
                        variant="body2"
                        sx={{
                          fontFamily: 'monospace',
                          fontSize: '0.65rem',
                          wordBreak: 'break-all',
                        }}
                      >
                        {externalAudioSegmentId}
                      </Typography>
                    </Box>
                  </Box>
                )}

                {degradationReasons.length > 0 && (
                  <Box sx={{ mt: 1 }}>
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ display: 'block', mb: 0.5 }}
                    >
                      Segment error(s)
                    </Typography>
                    {degradationReasons.map((error, index) => (
                      <Typography
                        key={index}
                        variant="body2"
                        color="error.main"
                        sx={{ wordBreak: 'break-word' }}
                      >
                        {error}
                      </Typography>
                    ))}
                  </Box>
                )}
              </Box>
            </>
          )}
        </Box>
      </Popover>
    </>
  );
}

export default SegmentInfoPopover;
