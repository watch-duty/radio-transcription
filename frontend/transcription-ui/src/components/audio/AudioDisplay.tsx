import { useMemo, useState, useEffect } from 'react';
import WavesurferPlayer from '@wavesurfer/react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Paper from '@mui/material/Paper';
import { useTheme } from '@mui/material/styles';
import WarningAmber from '@mui/icons-material/WarningAmber';
import type { Transcript } from '@transcription/common';
import { getAudioUrl } from '../../utils/audioUtils';

interface AudioDisplayProps {
  transcripts: Transcript[];
  currentlyPlayingTransmissionId: string | null;
  onClipClick?: (transmissionId: string) => void;
}

const MAX_WINDOW_DURATION_MS = 15 * 60 * 1000; // 15 minutes

const formatTime = (timestamp: number) => {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
};

export function AudioDisplay({
  transcripts,
  currentlyPlayingTransmissionId,
  onClipClick,
}: AudioDisplayProps) {
  const theme = useTheme();

  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  useEffect(() => {
    if (transcripts.length > 0 && windowEndTime === null) {
      setWindowEndTime(new Date(transcripts[0].endTimestamp).getTime());
    }
  }, [transcripts, windowEndTime]);

  useEffect(() => {
    if (!currentlyPlayingTransmissionId) return;
    
    const playingTranscript = transcripts.find(t => t.transmissionId === currentlyPlayingTransmissionId);
    if (!playingTranscript) return;
    
    const tStart = new Date(playingTranscript.startTimestamp).getTime();
    const tEnd = new Date(playingTranscript.endTimestamp).getTime();
    
    const currentEndTime = windowEndTime || (transcripts[0] ? new Date(transcripts[0].endTimestamp).getTime() : Date.now());
    const currentStartTime = currentEndTime - MAX_WINDOW_DURATION_MS;
    
    // If playing transcript is out of bounds, shift window
    if (tStart < currentStartTime || tEnd > currentEndTime) {
      // Center it
      const newEndTime = tStart + (MAX_WINDOW_DURATION_MS / 2);
      setWindowEndTime(newEndTime);
    }
  }, [currentlyPlayingTransmissionId, transcripts, windowEndTime]);

  const { startTime, windowDuration, clips } = useMemo(() => {
    if (transcripts.length === 0) {
      return {
        startTime: Date.now(),
        windowDuration: MAX_WINDOW_DURATION_MS,
        clips: [],
      };
    }

    const mostRecentTime = windowEndTime || new Date(transcripts[0].endTimestamp).getTime();
    const windowDuration = MAX_WINDOW_DURATION_MS;
    const startTime = mostRecentTime - windowDuration;

    const clips = transcripts
      .filter(t => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < startTime + windowDuration && tEnd > startTime;
      })
      .map(t => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        
        // Constrain to window bounds
        const visibleStart = Math.max(tStart, startTime);
        const visibleEnd = Math.min(tEnd, startTime + windowDuration);
        
        const left = ((visibleStart - startTime) / windowDuration) * 100;
        const width = ((visibleEnd - visibleStart) / windowDuration) * 100;
        
        const url = getAudioUrl(t.canonicalAudioUri);

        return {
          id: t.transmissionId,
          url,
          left,
          width,
          isPlaying: t.transmissionId === currentlyPlayingTransmissionId,
          hasAlert: t.evaluationDecisions && t.evaluationDecisions.length > 0,
        };
      });

    return { startTime, windowDuration, clips };
  }, [transcripts, currentlyPlayingTransmissionId, windowEndTime]);

  return (
    <Box sx={{ width: '100%', mb: 2 }}>
      <Paper
        variant="outlined"
        sx={{
          width: '100%',
          height: '60px',
          bgcolor: 'action.hover',
          position: 'relative',
        }}
      >
        {clips.map(clip => (
          <Box
            key={clip.id}
            onClick={() => onClipClick?.(clip.id)}
            sx={{
              position: 'absolute',
              left: `${clip.left}%`,
              width: `${clip.width}%`,
              height: '100%',
              bgcolor: clip.isPlaying ? 'rgba(0, 0, 0, 0.05)' : 'transparent',
              cursor: 'pointer',
              '&:hover': {
                bgcolor: clip.isPlaying ? 'rgba(0, 0, 0, 0.1)' : 'rgba(0, 0, 0, 0.03)',
              },
            }}
          >
            {clip.hasAlert && (
              <WarningAmber
                color="warning"
                fontSize="small"
                data-testid="warning-icon"
                sx={{
                  position: 'absolute',
                  left: 0,
                  top: -22,
                  zIndex: 1,
                  bgcolor: 'background.paper',
                  borderRadius: '50%',
                }}
              />
            )}
            <WavesurferPlayer
              url={clip.url}
              waveColor={theme.palette.text.secondary}
              progressColor={theme.palette.text.primary}
              cursorColor="transparent"
              barWidth={1}
              barGap={1}
              height={60}
            />
          </Box>
        ))}
        {transcripts.length === 0 && (
          <Box sx={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)', width: '100%', textAlign: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              No transcripts loaded
            </Typography>
          </Box>
        )}
      </Paper>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 0.5 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <Typography key={i} variant="caption" color="text.secondary">
            {formatTime(startTime + (i / 3) * windowDuration)}
          </Typography>
        ))}
      </Box>
    </Box>
  );
}

export default AudioDisplay;
