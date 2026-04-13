import { useState, Fragment } from 'react';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import IconButton from '@mui/material/IconButton';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import Paper from '@mui/material/Paper';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import ThumbUpIcon from '@mui/icons-material/ThumbUp';
import ThumbDownIcon from '@mui/icons-material/ThumbDown';

import { useAuth } from '../../context/AuthContext';
import {
  type Transcript,
  listTranscripts,
} from '../../service/listTranscripts';
import AudioPlayer from '../audio/AudioPlayer';

export function TranscriptView() {
  const [feedId, setFeedId] = useState('');
  const [transcripts, setTranscripts] = useState<Transcript[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { token } = useAuth();

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);

  const handleFetch = async () => {
    if (!feedId.trim()) return;
    setTranscripts([]);
    setLoading(true);
    setError(null);

    try {
      const transcripts = await listTranscripts(feedId, token!);
      setTranscripts(transcripts);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('An unknown error occurred');
      }
    } finally {
      setLoading(false);
    }
  };

  const onPlay = (transmissionId: string | null) => {
    setCurrentlyPlayingTransmissionId(transmissionId);
  };

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        height: 'calc(100vh - 112px)',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <Typography variant="h5" gutterBottom>
        View Transcripts
      </Typography>

      <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center' }}>
        <TextField
          label="Enter Feed ID"
          variant="outlined"
          value={feedId}
          onChange={(e) => setFeedId(e.target.value)}
          sx={{ flexGrow: 1 }}
          size="small"
        />
        <Button
          variant="contained"
          onClick={handleFetch}
          disabled={loading || !feedId.trim()}
          sx={{ minWidth: '100px' }}
        >
          {loading ? <CircularProgress size={24} color="inherit" /> : 'Fetch'}
        </Button>
      </Box>

      {error && (
        <Typography color="error" sx={{ mb: 2 }}>
          {error}
        </Typography>
      )}

      <Box sx={{ flexGrow: 1, overflowY: 'auto' }}>
        {transcripts.length > 0 ? (
          <List component={Paper} variant="outlined" sx={{ p: 0 }}>
            {transcripts.map((t, index) => {
              const currentDate = new Date(t.startTimestamp);
              const prevDate = index > 0 ? new Date(transcripts[index - 1].startTimestamp) : null;
              const showHeader = !prevDate || currentDate.toDateString() !== prevDate.toDateString();

              return (
                <Fragment key={t.transmissionId}>
                  {showHeader && (
                    <ListItem sx={{ py: 0.5, bgcolor: 'action.hover' }}>
                      <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 'bold' }}>
                        {currentDate.toLocaleDateString([], { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}
                      </Typography>
                    </ListItem>
                  )}
                  <ListItem
                    divider={index < transcripts.length - 1}
                    sx={{ display: 'flex', alignItems: 'center', gap: 2, py: 1.5 }}
                  >
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ minWidth: 'max-content' }}
                    >
                      {currentDate.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZoneName: 'short', hour12: false })}
                    </Typography>
                    <AudioPlayer
                      audioUri={t.canonicalAudioUri}
                      transmissionId={t.transmissionId}
                      onPlay={onPlay}
                      currentlyPlayingTransmissionId={
                        currentlyPlayingTransmissionId
                      }
                    />
                    <Typography
                      variant="body1"
                      sx={{ flexGrow: 1, whiteSpace: 'pre-wrap' }}
                    >
                      {t.transcript}
                    </Typography>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <IconButton size="small" aria-label="thumbs up" disabled>
                        <ThumbUpIcon fontSize="small" />
                      </IconButton>
                      <IconButton size="small" aria-label="thumbs down" disabled>
                        <ThumbDownIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </ListItem>
                </Fragment>
              );
            })}
          </List>
        ) : (
          !loading &&
          !error && (
            <Typography color="text.secondary" align="center" sx={{ mt: 4 }}>
              Enter a Feed ID and click Fetch to see transcripts.
            </Typography>
          )
        )}
      </Box>
    </Box>
  );
}

export default TranscriptView;
