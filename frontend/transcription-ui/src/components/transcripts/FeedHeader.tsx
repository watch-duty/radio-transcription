import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Link from '@mui/material/Link';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { Feed, FeedStatus } from '@transcription/common';

import { getRelativeTimeString } from '../../utils/timeUtils';

interface FeedHeaderProps {
  searchedFeed: Feed | null;
  sourceUrl?: string;
  archiveUrl?: string;
  status?: FeedStatus;
  lastHeartbeat?: string;
  triggerSnackbar: (message: string) => void;
}

const FEED_STATUS_UI_CONFIG: Record<
  FeedStatus,
  { displayText: string; color: 'success' | 'error' }
> = {
  active: { displayText: 'Active', color: 'success' },
  inactive: { displayText: 'Inactive', color: 'error' },
};

function FeedStatusIndicator({
  status,
  lastHeartbeat,
}: {
  status: FeedStatus | undefined;
  lastHeartbeat?: string;
}) {
  if (!status) {
    return null;
  }

  const statusConfig = status ? FEED_STATUS_UI_CONFIG[status] : undefined;
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
      <Badge
        color={statusConfig?.color ?? 'error'}
        variant="dot"
        sx={{
          py: 0,
          px: 0.5,
          display: 'flex',
          alignItems: 'center',
        }}
      ></Badge>
      <Typography
        variant="body2"
        sx={{
          color: `${statusConfig?.color ?? 'error'}.main`,
          fontWeight: 600,
          textTransform: 'uppercase',
        }}
      >
        {statusConfig?.displayText ?? status}
      </Typography>
      {lastHeartbeat && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ whiteSpace: 'nowrap' }}
        >
          Last updated: {getRelativeTimeString(lastHeartbeat)}
        </Typography>
      )}
    </Box>
  );
}

const FeedHeader: React.FC<FeedHeaderProps> = ({
  searchedFeed,
  sourceUrl,
  archiveUrl,
  status,
  lastHeartbeat,
  triggerSnackbar,
}) => {
  if (!searchedFeed) {
    return null;
  }

  return (
    <Box
      sx={{
        mb: 2,
        display: 'flex',
        flexDirection: 'row',
        justifyContent: 'space-between',
        alignItems: 'center',
        flexWrap: 'wrap',
        gap: 2,
        width: '100%',
      }}
    >
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'row',
          alignItems: 'center',
          gap: 1,
        }}
      >
        <Typography>
          <b>{searchedFeed.name}</b>
        </Typography>{' '}
        <FeedStatusIndicator status={status} lastHeartbeat={lastHeartbeat} />
      </Box>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
        }}
      >
        <Tooltip title="Copy feed deep link">
          <Button
            variant="outlined"
            size="small"
            disabled={!searchedFeed}
            onClick={() => {
              if (!searchedFeed) {
                return;
              }

              const url = new URL(
                window.location.origin + window.location.pathname
              );
              url.searchParams.set('feedId', searchedFeed.id);
              navigator.clipboard.writeText(url.toString());
              triggerSnackbar('Feed link copied');
            }}
            sx={{ textTransform: 'none', cursor: 'copy' }}
            aria-label="copy feed deeplink"
            startIcon={<LinkIcon fontSize="small" />}
          >
            Share feed
          </Button>
        </Tooltip>
        {sourceUrl && (
          <Link
            href={sourceUrl}
            target="_blank"
            rel="noopener noreferrer"
            variant="body2"
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 0.5,
            }}
          >
            <OpenInNewOutlinedIcon fontSize="small" />
            Original source link
          </Link>
        )}
        {archiveUrl && (
          <Link
            href={archiveUrl}
            target="_blank"
            rel="noopener noreferrer"
            variant="body2"
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 0.5,
            }}
          >
            <InventoryIcon fontSize="small" />
            Archives link
          </Link>
        )}
      </Box>
    </Box>
  );
};

export default FeedHeader;
