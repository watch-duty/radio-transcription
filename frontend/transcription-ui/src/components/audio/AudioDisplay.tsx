import { useMemo, useState } from 'react';

import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import type { Transcript } from '@transcription/common';
import WavesurferPlayer from '@wavesurfer/react';

import FireIcon from '../../assets/FireIcon';
import { getAudioUrl } from '../../utils/audioUtils';
import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';

interface AudioDisplayProps {
  transcripts: Transcript[];
  currentlyPlayingTransmissionId: string | null;
  onClipClick?: (transmissionId: string) => void;
  userDuration?: string | null;
}

const formatTime = (timestamp: number) => {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
};

export function AudioDisplay({
  transcripts,
  currentlyPlayingTransmissionId,
  onClipClick,
  userDuration,
}: AudioDisplayProps) {
  const theme = useTheme();

  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  const [prevFirstTranscriptId, setPrevFirstTranscriptId] = useState<
    string | null
  >(null);
  const [prevPlayingId, setPrevPlayingId] = useState<string | null>(null);
  const [prevUserDuration, setPrevUserDuration] = useState<string | null>(
    userDuration ?? null
  );

  const windowDurationMs = useMemo(() => {
    if (!userDuration) return MAX_WINDOW_DURATION_MS;
    const parsed = parseFloat(userDuration);
    if (isNaN(parsed) || parsed <= 0) return MAX_WINDOW_DURATION_MS;
    const userMs = parsed * 2 * 60 * 1000;
    return Math.min(MAX_WINDOW_DURATION_MS, userMs);
  }, [userDuration]);

  const firstTranscript = transcripts[0];
  const firstTranscriptId = firstTranscript?.transmissionId || null;
  const firstTranscriptEndTimestamp = firstTranscript?.endTimestamp || null;

  // Reset windowEndTime when first transcript changes
  if (firstTranscriptId !== prevFirstTranscriptId) {
    setPrevFirstTranscriptId(firstTranscriptId);
    setWindowEndTime(
      firstTranscriptEndTimestamp
        ? new Date(firstTranscriptEndTimestamp).getTime()
        : null
    );
    setPrevPlayingId(null); // Force re-check of bounds
  }

  const playingId = currentlyPlayingTransmissionId || null;

  // Shift windowEndTime when playing transcript goes out of bounds
  if (
    playingId !== prevPlayingId ||
    (userDuration ?? null) !== prevUserDuration
  ) {
    setPrevPlayingId(playingId);
    setPrevUserDuration(userDuration ?? null);
    if (playingId) {
      const playingTranscript = transcripts.find(
        (t) => t.transmissionId === playingId
      );
      if (playingTranscript) {
        const tStart = new Date(playingTranscript.startTimestamp).getTime();
        const tEnd = new Date(playingTranscript.endTimestamp).getTime();

        const currentEndTime =
          windowEndTime ||
          (firstTranscript
            ? new Date(firstTranscript.endTimestamp).getTime()
            : 0);
        const currentStartTime = currentEndTime - windowDurationMs;

        if (tStart < currentStartTime || tEnd > currentEndTime) {
          const newEndTime = tStart + windowDurationMs / 2;
          setWindowEndTime(newEndTime);
        }
      }
    }
  }

  // Calculates the visible time window bounds and processes transcripts into positioned clips for the waveform display.
  const { startTime, windowDuration, clips } = useMemo(() => {
    if (transcripts.length === 0) {
      return {
        startTime: 0,
        windowDuration: windowDurationMs,
        clips: [],
      };
    }

    const mostRecentTime =
      windowEndTime || new Date(transcripts[0].endTimestamp).getTime();
    const windowDuration = windowDurationMs;
    const startTime = mostRecentTime - windowDuration;

    // Filter for transcripts that overlap with the current visible time window
    const clips = transcripts
      .filter((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < startTime + windowDuration && tEnd > startTime;
      })
      // Map filtered transcripts to clip objects with calculated positioning and display properties
      .map((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();

        // Constrain to window bounds
        const visibleStart = Math.max(tStart, startTime);
        const visibleEnd = Math.min(tEnd, startTime + windowDuration);

        const left = ((visibleStart - startTime) / windowDuration) * 100;
        const width = ((visibleEnd - visibleStart) / windowDuration) * 100;

        const url = getAudioUrl(t.playbackAudioUri);

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
  }, [
    transcripts,
    currentlyPlayingTransmissionId,
    windowEndTime,
    windowDurationMs,
  ]);

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
        {clips.map((clip) => (
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
                bgcolor: clip.isPlaying
                  ? 'rgba(0, 0, 0, 0.1)'
                  : 'rgba(0, 0, 0, 0.03)',
              },
            }}
          >
            {clip.hasAlert && (
              <FireIcon
                color="warning"
                fontSize="medium"
                data-testid="warning-icon"
                sx={{
                  position: 'absolute',
                  // This centers the icon over the audio start, rather than left-aligned at the audio start.
                  left: -11,
                  // This provides enough buffer to move the icon on top of the clip view rather than on it.
                  top: -25,
                  zIndex: 1,
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
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: '100%',
              textAlign: 'center',
            }}
          >
            <Typography variant="body2" color="text.secondary">
              No transcripts loaded
            </Typography>
          </Box>
        )}
      </Paper>
      {transcripts.length > 0 && (
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 0.5 }}>
          {Array.from({ length: 4 }).map((_, i) => (
            <Typography key={i} variant="caption" color="text.secondary">
              {formatTime(startTime + (i / 3) * windowDuration)}
            </Typography>
          ))}
        </Box>
      )}
    </Box>
  );
}

export default AudioDisplay;
