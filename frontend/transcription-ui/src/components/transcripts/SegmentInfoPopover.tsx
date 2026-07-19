import React, { useState } from 'react';

import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Popover from '@mui/material/Popover';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { AudioSegment } from '@transcription/common';

interface SegmentInfoPopoverProps {
  audioSegment: AudioSegment;
  triggerSnackbar: (message: string) => void;
  degradationReasons?: string[];
}

export function SegmentInfoPopover({
  audioSegment,
  triggerSnackbar,
  degradationReasons,
}: SegmentInfoPopoverProps) {
  const [infoAnchorEl, setInfoAnchorEl] = useState<HTMLElement | null>(null);

  const handleInfoClick = (event: React.MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    setInfoAnchorEl(event.currentTarget);
  };

  const handleInfoClose = () => {
    setInfoAnchorEl(null);
  };

  const { id, externalAudioSegmentId } = audioSegment;

  const open = Boolean(infoAnchorEl);
  const popoverId = open ? `segment-info-popover-${id}` : undefined;

  return (
    <>
      <Tooltip title="View segment info">
        <IconButton
          size="small"
          aria-label="view segment info"
          aria-describedby={popoverId}
          onClick={handleInfoClick}
        >
          <InfoOutlinedIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Popover
        id={popoverId}
        open={open}
        anchorEl={infoAnchorEl}
        onClose={handleInfoClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <Box
          sx={{
            p: 2,
            display: 'flex',
            flexDirection: 'column',
            gap: 1.5,
            minWidth: 250,
          }}
        >
          <Typography variant="subtitle2" sx={{ fontWeight: 'bold' }}>
            Segment Details
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 2,
              }}
            >
              <Box>
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
                    wordBreak: 'break-all',
                  }}
                >
                  {id}
                </Typography>
              </Box>
              <Tooltip title="Copy Segment ID">
                <IconButton
                  size="small"
                  aria-label="copy segment id"
                  onClick={(e) => {
                    e.stopPropagation();
                    navigator.clipboard.writeText(id);
                    triggerSnackbar('Segment ID copied');
                  }}
                >
                  <ContentCopyIcon fontSize="inherit" />
                </IconButton>
              </Tooltip>
            </Box>

            {externalAudioSegmentId && (
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 2,
                }}
              >
                <Box>
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
                      wordBreak: 'break-all',
                    }}
                  >
                    {externalAudioSegmentId}
                  </Typography>
                </Box>
                <Tooltip title="Copy External ID">
                  <IconButton
                    size="small"
                    aria-label="copy external segment id"
                    onClick={(e) => {
                      e.stopPropagation();
                      navigator.clipboard.writeText(externalAudioSegmentId);
                      triggerSnackbar('External segment ID copied');
                    }}
                  >
                    <ContentCopyIcon fontSize="inherit" />
                  </IconButton>
                </Tooltip>
              </Box>
            )}

            {degradationReasons && degradationReasons.length > 0 && (
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
                    sx={{
                      wordBreak: 'break-word',
                    }}
                  >
                    {error}
                  </Typography>
                ))}
              </Box>
            )}
          </Box>
        </Box>
      </Popover>
    </>
  );
}
