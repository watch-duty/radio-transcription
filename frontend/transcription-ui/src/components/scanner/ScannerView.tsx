import { useEffect, useMemo, useRef, useState } from 'react';

import AddIcon from '@mui/icons-material/Add';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import GraphicEqIcon from '@mui/icons-material/GraphicEq';
import PauseCircleFilledOutlinedIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayCircleFilledOutlinedIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import VolumeOffIcon from '@mui/icons-material/VolumeOff';
import VolumeUpIcon from '@mui/icons-material/VolumeUp';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Checkbox from '@mui/material/Checkbox';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';
import Divider from '@mui/material/Divider';
import FormControlLabel from '@mui/material/FormControlLabel';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useQuery } from '@tanstack/react-query';
import { type Feed } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useScanner } from '../../context/ScannerContext';
import {
  ScannerPlaybackContext,
  ScannerPlaybackStateContext,
  useScannerPlayback,
} from '../../context/ScannerPlaybackContext';
import { useWakeLock } from '../../hooks/useWakeLock';
import { listRules } from '../../service/listRules';
import FeedSearchView from '../feeds/FeedSearchView';
import ScannerPanel from './ScannerPanel';

interface ScannerViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

// Shared by the add-feed row and the panel grid so the add controls track the
// first panel column across breakpoints.
const SCANNER_GRID_COLUMNS = {
  xs: '1fr',
  md: 'repeat(2, 1fr)',
  xl: 'repeat(3, 1fr)',
};
const SCANNER_GRID_GAP = 2;

export function ScannerView({ triggerSnackbar, onError }: ScannerViewProps) {
  const { token } = useAuth();
  const { scannerFeeds, reorderFeeds, removeAll, addFeed, isInScanner } =
    useScanner();

  const { coordinator, state: playbackState } = useScannerPlayback();
  useWakeLock(playbackState.anyPlaying);

  const dragFromIndex = useRef<number | null>(null);
  const [dropTargetIndex, setDropTargetIndex] = useState<number | null>(null);
  const [confirmRemoveAllOpen, setConfirmRemoveAllOpen] = useState(false);
  // Feed chosen in the selector, staged until the Add button commits it.
  const [pendingFeed, setPendingFeed] = useState<Feed | null>(null);

  const {
    data: rules,
    error: rulesError,
    isLoading: rulesLoading,
  } = useQuery({
    queryKey: ['listRules', token],
    queryFn: () => listRules(token ?? ''),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (rulesError) {
      onError(rulesError, 'Loading rules');
    }
  }, [rulesError, onError]);

  const ruleIdToNameMap = useMemo(() => {
    if (!rules) return new Map<string, string>();
    return new Map(rules.map((rule) => [rule.ruleId, rule.ruleName]));
  }, [rules]);

  const handleAddPendingFeed = () => {
    if (!pendingFeed) return;
    if (isInScanner(pendingFeed.id)) {
      triggerSnackbar(`${pendingFeed.name} is already in your Scanner`);
    } else {
      addFeed({ id: pendingFeed.id, name: pendingFeed.name });
      triggerSnackbar(`Added ${pendingFeed.name} to Scanner`);
    }
    setPendingFeed(null);
  };

  const handleDrop = (toIndex: number) => {
    const fromIndex = dragFromIndex.current;
    if (fromIndex !== null) {
      reorderFeeds(fromIndex, toIndex);
    }
    dragFromIndex.current = null;
    setDropTargetIndex(null);
  };

  if (!token) {
    return null;
  }

  const isEmpty = scannerFeeds.length === 0;

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <GraphicEqIcon color="primary" />
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          Scanner
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {scannerFeeds.length} feed{scannerFeeds.length === 1 ? '' : 's'}
        </Typography>
        <Box
          sx={{
            ml: 'auto',
            display: 'flex',
            alignItems: 'center',
            gap: 0.5,
            flexWrap: 'wrap',
            justifyContent: 'flex-end',
          }}
        >
          {!isEmpty && (
            <>
              <Tooltip title="Play all">
                <IconButton
                  size="large"
                  color="primary"
                  onClick={() => coordinator.playAll()}
                  aria-label="Play all"
                >
                  <PlayCircleFilledOutlinedIcon fontSize="large" />
                </IconButton>
              </Tooltip>
              <Tooltip title="Pause all">
                <IconButton
                  size="large"
                  color="primary"
                  onClick={() => coordinator.pauseAll()}
                  aria-label="Pause all"
                >
                  <PauseCircleFilledOutlinedIcon fontSize="large" />
                </IconButton>
              </Tooltip>
              <Tooltip
                title={playbackState.muteAll ? 'Unmute all' : 'Mute all'}
              >
                <IconButton
                  size="large"
                  color="primary"
                  onClick={() => coordinator.setMuteAll(!playbackState.muteAll)}
                  aria-label={playbackState.muteAll ? 'Unmute all' : 'Mute all'}
                >
                  {playbackState.muteAll ? (
                    <VolumeOffIcon fontSize="large" />
                  ) : (
                    <VolumeUpIcon fontSize="large" />
                  )}
                </IconButton>
              </Tooltip>
              <Tooltip title="Remove all">
                <IconButton
                  size="large"
                  color="error"
                  onClick={() => setConfirmRemoveAllOpen(true)}
                  aria-label="Remove all"
                >
                  <DeleteSweepIcon fontSize="large" />
                </IconButton>
              </Tooltip>
              <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
            </>
          )}
          <Tooltip title="Play one feed at a time">
            <FormControlLabel
              sx={{ mr: 0 }}
              control={
                <Checkbox
                  size="small"
                  checked={playbackState.sequentialEnabled}
                  onChange={(e) =>
                    coordinator.setSequentialEnabled(e.target.checked)
                  }
                />
              }
              label="One at a time"
            />
          </Tooltip>
        </Box>
      </Box>

      <Box
        sx={{
          mb: 2,
          display: 'grid',
          gridTemplateColumns: SCANNER_GRID_COLUMNS,
          gap: SCANNER_GRID_GAP,
        }}
      >
        <Box
          sx={{
            gridColumn: '1',
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            minWidth: 0,
          }}
        >
          <FeedSearchView
            title="Add feed"
            condensed
            fullWidth
            retainSelection
            value={pendingFeed}
            onFeedSelect={setPendingFeed}
            triggerSnackbar={triggerSnackbar}
            onError={onError}
          />
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            disabled={!pendingFeed}
            onClick={handleAddPendingFeed}
            sx={{ flexShrink: 0 }}
          >
            Add
          </Button>
        </Box>
      </Box>

      <Dialog
        open={confirmRemoveAllOpen}
        onClose={() => setConfirmRemoveAllOpen(false)}
      >
        <DialogTitle>Remove all feeds from Scanner?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            This removes all {scannerFeeds.length} feed
            {scannerFeeds.length === 1 ? '' : 's'} from your Scanner. It
            won&apos;t delete the feeds themselves — you can add them back from
            the Feeds list.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmRemoveAllOpen(false)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={() => {
              removeAll();
              setConfirmRemoveAllOpen(false);
            }}
          >
            Remove all
          </Button>
        </DialogActions>
      </Dialog>
      {isEmpty ? (
        <Box
          sx={{
            flexGrow: 1,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            textAlign: 'center',
            color: 'text.secondary',
            px: 3,
          }}
        >
          <GraphicEqIcon color="disabled" sx={{ fontSize: 48, mb: 1 }} />
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            No feeds in your Scanner yet
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Use the &ldquo;Add feed&rdquo; selector above, or add feeds from the
            Feeds list.
          </Typography>
        </Box>
      ) : (
        <ScannerPlaybackContext.Provider value={coordinator}>
          <ScannerPlaybackStateContext.Provider value={playbackState}>
            <Box
              sx={{
                display: 'grid',
                gridTemplateColumns: SCANNER_GRID_COLUMNS,
                gap: SCANNER_GRID_GAP,
              }}
            >
              {scannerFeeds.map((feed, index) => (
                <ScannerPanel
                  key={feed.id}
                  feed={feed}
                  index={index}
                  token={token}
                  ruleIdToNameMap={ruleIdToNameMap}
                  rulesLoading={rulesLoading}
                  triggerSnackbar={triggerSnackbar}
                  onDragStart={(i) => {
                    dragFromIndex.current = i;
                  }}
                  onDragOver={(i) => setDropTargetIndex(i)}
                  onDrop={handleDrop}
                  isDropTarget={dropTargetIndex === index}
                />
              ))}
            </Box>
          </ScannerPlaybackStateContext.Provider>
        </ScannerPlaybackContext.Provider>
      )}
    </Box>
  );
}

export default ScannerView;
