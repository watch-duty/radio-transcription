import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { AudioSegment } from '@transcription/common';

interface SegmentDetailsProps {
  audioSegment: AudioSegment;
  triggerSnackbar: (message: string) => void;
  degradationReasons?: string[];
}

// Segment metadata shown inline in the row's share popover (admins only).
export function SegmentDetails({
  audioSegment,
  triggerSnackbar,
  degradationReasons,
}: SegmentDetailsProps) {
  const { id, externalAudioSegmentId } = audioSegment;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, pl: 1 }}>
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
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, pl: 1 }}>
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
                sx={{ wordBreak: 'break-word' }}
              >
                {error}
              </Typography>
            ))}
          </Box>
        )}
      </Box>
    </Box>
  );
}

export default SegmentDetails;
