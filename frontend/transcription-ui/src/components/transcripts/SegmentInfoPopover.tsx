import { useState } from 'react';

import { saveAs } from 'file-saver';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DownloadIcon from '@mui/icons-material/Download';
import InfoIcon from '@mui/icons-material/InfoOutlineRounded';
import LinkIcon from '@mui/icons-material/Link';
import MoreHorizIcon from '@mui/icons-material/MoreHoriz';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Popover from '@mui/material/Popover';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  AudioSegment,
  TranscriptAnnotationData,
} from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { getAudioUrl } from '../../utils/audioUtils';

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
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const [infoAnchor, setInfoAnchor] = useState<HTMLElement | null>(null);
  const closeMenu = () => setMenuAnchor(null);
  const closeInfo = () => setInfoAnchor(null);

  const handleCopyTranscript = () => {
    if (transcriptAnnotation?.text) {
      navigator.clipboard.writeText(transcriptAnnotation.text);
      triggerSnackbar('Transcript copied');
    }
    closeMenu();
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
    closeMenu();
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
    closeMenu();
  };

  const handleCopySegmentId = () => {
    navigator.clipboard.writeText(id);
    triggerSnackbar('Segment ID copied');
    closeInfo();
  };

  const handleCopyExternalSegmentId = () => {
    if (externalAudioSegmentId) {
      navigator.clipboard.writeText(externalAudioSegmentId);
      triggerSnackbar('External ID copied');
      closeInfo();
    }
  };

  return (
    <>
      <Tooltip title="Share">
        <IconButton
          size="small"
          aria-label="Share"
          aria-haspopup="dialog"
          aria-expanded={Boolean(menuAnchor)}
          onClick={(e) => {
            e.stopPropagation();
            setMenuAnchor(e.currentTarget);
          }}
        >
          <MoreHorizIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Menu
        open={Boolean(menuAnchor)}
        anchorEl={menuAnchor}
        onClose={closeMenu}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
        onClick={(e) => e.stopPropagation()}
      >
        {!isSilence && !isOutage && (
          <MenuItem
            onClick={handleCopyTranscript}
            disabled={!transcriptAnnotation || hasErrors}
            dense
          >
            <ListItemIcon>
              <ContentCopyIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText>Copy transcript</ListItemText>
          </MenuItem>
        )}
        <MenuItem onClick={handleCopyLink} dense>
          <ListItemIcon>
            <LinkIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Copy link</ListItemText>
        </MenuItem>
        <MenuItem
          onClick={handleDownloadAudio}
          disabled={!audioSegment.playbackAudioUri}
          dense
        >
          <ListItemIcon>
            <DownloadIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Download audio</ListItemText>
        </MenuItem>
      </Menu>

      {isAdmin && (
        <>
          <Tooltip title="Segment info">
            <IconButton
              size="small"
              aria-label="Segment info"
              aria-haspopup="dialog"
              aria-expanded={Boolean(infoAnchor)}
              onClick={(e) => {
                e.stopPropagation();
                setInfoAnchor(e.currentTarget);
              }}
            >
              <InfoIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Popover
            open={Boolean(infoAnchor)}
            anchorEl={infoAnchor}
            onClose={closeInfo}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            onClick={(e) => e.stopPropagation()}
          >
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                gap: 1,
                p: 1,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Tooltip title="Copy Segment ID">
                  <IconButton
                    size="small"
                    aria-label="copy segment id"
                    onClick={handleCopySegmentId}
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
                  }}
                >
                  <Tooltip title="Copy External ID">
                    <IconButton
                      size="small"
                      aria-label="copy external segment id"
                      onClick={handleCopyExternalSegmentId}
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
          </Popover>
        </>
      )}
    </>
  );
}

export default SegmentInfoPopover;
