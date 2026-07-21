import { useRef } from 'react';
import { Link as RouterLink } from 'react-router';

import CloseIcon from '@mui/icons-material/Close';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import HeadphonesIcon from '@mui/icons-material/Headphones';
import HeadphonesOutlinedIcon from '@mui/icons-material/HeadphonesOutlined';
import LinkIcon from '@mui/icons-material/Link';
import StarIcon from '@mui/icons-material/Star';
import StarBorderIcon from '@mui/icons-material/StarBorder';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import Tooltip from '@mui/material/Tooltip';
import { useQuery } from '@tanstack/react-query';

import { type ScannerFeed, useScanner } from '../../context/ScannerContext';
import {
  feedPlaybackView,
  useScannerPlaybackCoordinator,
  useScannerPlaybackState,
} from '../../context/ScannerPlaybackContext';
import { getFeed } from '../../service/getFeed';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import FeedPanel from '../feeds/FeedPanel';

const FEED_POLLING_INTERVAL_MS = 15000;

interface ScannerPanelProps {
  feed: ScannerFeed;
  token: string;
  index: number;
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
  triggerSnackbar: (message: string) => void;
  onDragStart: (index: number) => void;
  onDragOver: (index: number) => void;
  onDrop: (index: number) => void;
  isDropTarget: boolean;
}

export function ScannerPanel({
  feed,
  token,
  index,
  ruleIdToNameMap,
  rulesLoading,
  triggerSnackbar,
  onDragStart,
  onDragOver,
  onDrop,
  isDropTarget,
}: ScannerPanelProps) {
  const { removeFeed } = useScanner();
  const coordinator = useScannerPlaybackCoordinator();
  const playbackState = useScannerPlaybackState();
  const playbackView = feedPlaybackView(playbackState, feed.id);
  const cardRef = useRef<HTMLDivElement | null>(null);

  const { data: activeFeedData } = useQuery({
    queryKey: ['getFeed', token, feed.id],
    queryFn: () => getFeed(feed.id, token),
    enabled: !!token && !!feed.id,
    refetchInterval: FEED_POLLING_INTERVAL_MS,
    refetchOnWindowFocus: true,
  });

  const feedPath = `/transcripts?feedId=${feed.id}`;

  const handleShareFeed = () => {
    const url = new URL(window.location.origin + feedPath);
    navigator.clipboard.writeText(url.toString());
    triggerSnackbar('Feed link copied');
  };

  const starred = playbackView?.starred ?? false;
  const soloed = playbackView?.soloed ?? false;
  const status = playbackView?.status ?? 'idle';
  const statusChip =
    status === 'queued'
      ? {
          label: `Queued #${playbackView?.queuePosition ?? ''}`,
          color: 'default' as const,
        }
      : status === 'interrupted'
        ? { label: 'Paused', color: 'warning' as const }
        : null;

  // Use the whole card as the native drag image instead of the small handle.
  const handleDragStart = (e: React.DragEvent) => {
    if (cardRef.current) {
      const rect = cardRef.current.getBoundingClientRect();
      e.dataTransfer.setDragImage(
        cardRef.current,
        e.clientX - rect.left,
        e.clientY - rect.top
      );
    }
    onDragStart(index);
  };

  return (
    <Box
      ref={cardRef}
      onDragOver={(e) => {
        e.preventDefault();
        onDragOver(index);
      }}
      onDrop={(e) => {
        e.preventDefault();
        onDrop(index);
      }}
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: 520,
        border: '1px solid',
        borderColor: isDropTarget ? 'primary.main' : 'divider',
        borderRadius: 2,
        overflow: 'hidden',
        bgcolor: 'background.paper',
        boxShadow: '0 4px 20px rgba(0,0,0,0.05)',
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          px: 1.5,
          py: 1,
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
      >
        <Box
          draggable
          onDragStart={handleDragStart}
          sx={{ display: 'flex', cursor: 'grab', color: 'text.secondary' }}
          aria-label="Drag to reorder"
        >
          <DragIndicatorIcon fontSize="small" />
        </Box>
        <Link
          component={RouterLink}
          to={feedPath}
          variant="subtitle2"
          underline="hover"
          color="inherit"
          sx={{
            fontWeight: 700,
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            minWidth: 0,
            flexGrow: 1,
          }}
        >
          {feed.name}
        </Link>
        {statusChip && (
          <Chip
            size="small"
            variant="outlined"
            color={statusChip.color}
            label={statusChip.label}
            sx={{ height: 20, '& .MuiChip-label': { px: 0.75, fontSize: 11 } }}
          />
        )}
        {coordinator && (
          <>
            <Tooltip
              title={
                starred
                  ? 'Priority on — plays over other feeds'
                  : 'Priority — play over other feeds'
              }
            >
              <IconButton
                size="small"
                color={starred ? 'warning' : 'default'}
                onClick={() => coordinator.togglePriority(feed.id)}
                aria-label={`Priority ${feed.name}`}
              >
                {starred ? (
                  <StarIcon fontSize="small" />
                ) : (
                  <StarBorderIcon fontSize="small" />
                )}
              </IconButton>
            </Tooltip>
            <Tooltip
              title={
                soloed
                  ? 'Soloed — other feeds muted'
                  : 'Solo — mute other feeds'
              }
            >
              <IconButton
                size="small"
                color={soloed ? 'primary' : 'default'}
                onClick={() => coordinator.toggleSolo(feed.id)}
                aria-label={`Solo ${feed.name}`}
              >
                {soloed ? (
                  <HeadphonesIcon fontSize="small" />
                ) : (
                  <HeadphonesOutlinedIcon fontSize="small" />
                )}
              </IconButton>
            </Tooltip>
          </>
        )}
        {activeFeedData?.status === 'error' && (
          <FeedStatusIndicator
            status={activeFeedData.status}
            substatus={activeFeedData.substatus}
            statusReason={activeFeedData.statusReason}
            statusReasonDetail={activeFeedData.statusReasonDetail}
            lastHeartbeat={activeFeedData.lastHeartbeat}
            lastSpeechSegmentTimestamp={
              activeFeedData.lastSpeechSegmentTimestamp
            }
          />
        )}
        <Tooltip title="Share Feed">
          <IconButton
            size="small"
            onClick={handleShareFeed}
            aria-label={`Share ${feed.name}`}
          >
            <LinkIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Remove from Scanner">
          <IconButton
            size="small"
            onClick={() => removeFeed(feed.id)}
            aria-label={`Remove ${feed.name} from Scanner`}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>

      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          flexGrow: 1,
          minHeight: 0,
          px: 1.5,
          pb: 1.5,
        }}
      >
        <FeedPanel
          feed={activeFeedData ?? null}
          feedId={feed.id}
          token={token}
          ruleIdToNameMap={ruleIdToNameMap}
          rulesLoading={rulesLoading}
          triggerSnackbar={triggerSnackbar}
        />
      </Box>
    </Box>
  );
}

export default ScannerPanel;
