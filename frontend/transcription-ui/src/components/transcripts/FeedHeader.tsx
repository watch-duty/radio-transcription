import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { Feed, FeedStatus } from '@transcription/common';

import { useIsNarrow } from '../../hooks/useIsNarrow';
import { toSourceTypeString } from '../../utils/textUtils';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import FeedSearchView from '../feeds/FeedSearchView';

interface FeedHeaderProps {
  searchedFeed: Feed | null;
  onSelectFeed: (feedId: string) => void;
  sourceUrl?: string;
  archiveUrl?: string;
  status?: FeedStatus;
  lastSpeechSegmentTimestamp?: number;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FeedHeader: React.FC<FeedHeaderProps> = ({
  searchedFeed,
  onSelectFeed,
  sourceUrl,
  archiveUrl,
  status,
  lastSpeechSegmentTimestamp,
  triggerSnackbar,
  onError,
}) => {
  // Narrow: collapse the action links to icons so they stay on the title's line.
  const isNarrow = useIsNarrow();

  const handleShareFeed = () => {
    if (!searchedFeed) {
      return;
    }
    const url = new URL(window.location.origin + window.location.pathname);
    url.searchParams.set('feedId', searchedFeed.id);
    navigator.clipboard.writeText(url.toString());
    triggerSnackbar('Feed link copied');
  };

  // Narrow shows an icon-only button; wide shows an icon + text link.
  const renderExternalLink = (
    href: string,
    icon: React.ReactNode,
    label: string
  ) =>
    isNarrow ? (
      <Tooltip title={label}>
        <IconButton
          size="small"
          color="primary"
          component="a"
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={label}
        >
          {icon}
        </IconButton>
      </Tooltip>
    ) : (
      <Link
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        variant="body2"
        sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}
      >
        {icon}
        {label}
      </Link>
    );

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
            flexWrap: { xs: 'nowrap', sm: 'wrap' },
            gap: { xs: 1, sm: 2 },
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
              // Clip rather than push the action icons off-screen when the row
              // can't wrap (xs) and the name/chip/status don't fit.
              overflow: 'hidden',
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
              status={status}
              substatus={searchedFeed.substatus}
              statusReason={searchedFeed.statusReason}
              statusReasonDetail={searchedFeed.statusReasonDetail}
              lastSpeechSegmentTimestamp={lastSpeechSegmentTimestamp}
            />
          </Box>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              flexWrap: 'wrap',
              justifyContent: 'flex-end',
              flexShrink: 0,
              gap: { xs: 0.5, sm: 2 },
            }}
          >
            {isNarrow ? (
              <Tooltip title="Share feed">
                <IconButton
                  size="small"
                  color="primary"
                  disabled={!searchedFeed}
                  onClick={handleShareFeed}
                  aria-label="copy feed deeplink"
                  sx={{ cursor: 'copy' }}
                >
                  <LinkIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            ) : (
              <Tooltip title="Copy feed deep link">
                <Button
                  variant="outlined"
                  size="small"
                  disabled={!searchedFeed}
                  onClick={handleShareFeed}
                  sx={{ textTransform: 'none', cursor: 'copy' }}
                  aria-label="copy feed deeplink"
                  startIcon={<LinkIcon fontSize="small" />}
                >
                  Share feed
                </Button>
              </Tooltip>
            )}
            {sourceUrl &&
              renderExternalLink(
                sourceUrl,
                <OpenInNewOutlinedIcon fontSize="small" />,
                'Original source link'
              )}
            {archiveUrl &&
              renderExternalLink(
                archiveUrl,
                <InventoryIcon fontSize="small" />,
                'Archives link'
              )}
          </Box>
        </Box>
      )}
    </>
  );
};

export default FeedHeader;
