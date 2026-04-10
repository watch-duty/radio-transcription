import { useState } from 'react';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import Paper from '@mui/material/Paper';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';

import { useAuth } from '../../context/AuthContext';
import {
  type Transcript,
  listTranscripts,
} from '../../service/listTranscripts';

export function TranscriptView() {
  const [feedId, setFeedId] = useState('');
  const [transcripts, setTranscripts] = useState<Transcript[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { token } = useAuth();

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
            {transcripts.map((t, index) => (
              <ListItem
                key={t.transmissionId}
                divider={index < transcripts.length - 1}
                alignItems="flex-start"
                sx={{ flexDirection: 'column', py: 2 }}
              >
                <Box
                  sx={{
                    width: '100%',
                    display: 'flex',
                    justifyContent: 'space-between',
                    mb: 1,
                  }}
                >
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{ fontWeight: 'bold' }}
                  >
                    Transmission ID: {t.transmissionId}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {new Date(t.startTimestamp).toLocaleString()}
                  </Typography>
                </Box>
                <Typography
                  variant="body1"
                  sx={{ whiteSpace: 'pre-wrap', width: '100%' }}
                >
                  {t.transcript}
                </Typography>
              </ListItem>
            ))}
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
