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
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: 'flex-start',
            columnGap: 2,
            rowGap: 0.5,
          }}
        >
          <Typography
            component="h1"
            sx={{
              fontWeight: 'bold',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              // Name holds its natural size next to the controls (no early
              // truncation); the controls wrap / the activity truncates first.
              flexShrink: 0,
              maxWidth: '100%',
            }}
          >
            {searchedFeed.name}
          </Typography>
          {/* All the controls wrap to the next line together and stay on one
              line there. Fills the row so chip+status sit next to the name and
              the actions are pushed right; the activity truncates if short. */}
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexGrow: 1,
              flexShrink: 0,
              columnGap: 1,
              maxWidth: '100%',
            }}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.75,
                flexShrink: 1,
                minWidth: 0,
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
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                flexShrink: 0,
                columnGap: 0.5,
              }}
            >
              {renderAction(
                <LinkIcon fontSize="small" />,
                'Share feed',
                {
                  component: 'button',
                  type: 'button',
                  onClick: handleShareFeed,
                },
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
        </Box>
      )}
    </>
  );
};

export default FeedHeader;
