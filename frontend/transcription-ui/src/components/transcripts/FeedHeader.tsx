import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import Link, { type LinkProps } from '@mui/material/Link';
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

  const renderAction = (
    icon: React.ReactNode,
    label: string,
    linkProps: LinkProps,
    tooltip: string = label
  ) => (
    <Tooltip title={tooltip}>
      <Link
        variant="body2"
        aria-label={label}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: isNarrow ? 0 : 0.5,
          p: 0.5,
        }}
        {...linkProps}
      >
        {icon}
        {!isNarrow && label}
      </Link>
    </Tooltip>
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
            width: '100%',
            display: 'flex',
            flexWrap: 'nowrap',
            alignItems: isNarrow ? 'flex-start' : 'center',
            justifyContent: 'space-between',
            columnGap: 2,
          }}
        >
          <Box
            sx={{
              // Narrow: name on its own line with the chip+status group stacked
              // below; wide: both inline. Same `isNarrow` trigger as the labels.
              display: 'flex',
              flexDirection: isNarrow ? 'column' : 'row',
              alignItems: isNarrow ? 'flex-start' : 'center',
              gap: isNarrow ? 0.25 : 1,
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
                // Keep a healthy minimum for the name; the chip+status group
                // gives (status truncates) before the name does.
                flexShrink: 1,
                minWidth: 200,
                maxWidth: '100%',
              }}
            >
              {searchedFeed.name}
            </Typography>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                flexShrink: 1,
                minWidth: 0,
                maxWidth: '100%',
              }}
            >
              <Chip
                label={toSourceTypeString(searchedFeed.sourceType)}
                size="small"
                sx={{ flexShrink: 0 }}
              />
              <FeedStatusIndicator
                status={status}
                substatus={searchedFeed.substatus}
                statusReason={searchedFeed.statusReason}
                statusReasonDetail={searchedFeed.statusReasonDetail}
                lastSpeechSegmentTimestamp={lastSpeechSegmentTimestamp}
              />
            </Box>
          </Box>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              flexShrink: 0,
              columnGap: 2,
            }}
          >
            {renderAction(
              <LinkIcon fontSize="small" />,
              'Share feed',
              { component: 'button', type: 'button', onClick: handleShareFeed },
              'Copy feed deep link'
            )}
            {sourceUrl &&
              renderAction(
                <OpenInNewOutlinedIcon fontSize="small" />,
                'Original source link',
                {
                  href: sourceUrl,
                  target: '_blank',
                  rel: 'noopener noreferrer',
                }
              )}
            {archiveUrl &&
              renderAction(
                <InventoryIcon fontSize="small" />,
                'Archives link',
                {
                  href: archiveUrl,
                  target: '_blank',
                  rel: 'noopener noreferrer',
                }
              )}
          </Box>
        </Box>
      )}
    </>
  );
};

export default FeedHeader;
