import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import Link from '@mui/material/Link';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { Feed, FeedStatus } from '@transcription/common';

import { toSourceTypeString } from '../../utils/textUtils';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import FeedSearchView from '../feeds/FeedSearchView';

interface FeedHeaderProps {
  searchedFeed: Feed | null;
  liveFeed?: Feed | null;
  onSelectFeed: (feedId: string) => void;
  sourceUrl?: string;
  archiveUrl?: string;
  status?: FeedStatus;
  lastHeartbeat?: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FeedHeader: React.FC<FeedHeaderProps> = ({
  searchedFeed,
  liveFeed,
  onSelectFeed,
  sourceUrl,
  archiveUrl,
  status,
  lastHeartbeat,
  triggerSnackbar,
  onError,
}) => {
  const statusFeed = liveFeed ?? searchedFeed;

  return (
    <>
      <FeedSearchView
        title="Select feed"
        condensed={true}
        onFeedSelect={onSelectFeed}
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />
      {searchedFeed && (
        <Box
          sx={{
            mt: 1,
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
              flexGrow: 1,
              minWidth: 0,
            }}
          >
            <Typography
              component="h1"
              sx={{
                fontWeight: 'bold',
                whiteSpace: 'nowrap',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                minWidth: 0,
              }}
            >
              {searchedFeed.name}
            </Typography>
            <Chip
              label={toSourceTypeString(searchedFeed.sourceType)}
              size="small"
            />
            <FeedStatusIndicator
              status={liveFeed?.status ?? status ?? searchedFeed.status}
              substatus={statusFeed?.substatus}
              statusReason={statusFeed?.statusReason}
              statusReasonDetail={statusFeed?.statusReasonDetail}
              lastHeartbeat={
                liveFeed?.lastHeartbeat ??
                lastHeartbeat ??
                searchedFeed.lastHeartbeat
              }
            />
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
      )}
    </>
  );
};

export default FeedHeader;
