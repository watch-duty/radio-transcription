import Badge, { type BadgeProps } from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '@transcription/common';

import RelativeTimeDisplay from './RelativeTimeDisplay';

const FEED_SUBSTATUS_UI_TEXT_DISPLAY: Record<BackendFeedStatus, string> = {
  unclaimed: 'Unclaimed',
  active: 'Active',
  failing: 'Failing',
  quarantined: 'Quarantined',
  deactivated: 'Deactivated',
};

const FEED_STATUS_REASON_UI_TEXT_DISPLAY: Record<
  BackendFeedStatusReason,
  string
> = {
  unknown: 'Unknown Status',
  pipeline_publish_after_bookmark_failed: 'Publish After Bookmark Failed',
  source_offline: 'Source Offline',
  source_unreachable: 'Source Unreachable',
  source_rate_limited: 'Source Rate Limited',
  system_authentication_failed: 'System Authentication Failed',
  system_configuration_invalid: 'System Configuration Invalid',
  system_source_configuration_invalid: 'System Source Configuration Invalid',
  system_runtime_configuration_invalid: 'System Runtime Configuration Invalid',
  system_credential_access_failed: 'System Credential Access Failed',
  system_source_payload_invalid: 'System Source Payload Invalid',
  system_collector_error: 'System Collector Error',
  system_pipeline_error: 'System Pipeline Error',
  system_unexpected_error: 'System Unexpected Error',
};

const FEED_STATUS_UI_CONFIG: Record<
  FeedStatus,
  { displayText: string; color: BadgeProps['color'] }
> = {
  active: { displayText: 'Active', color: 'success' },
  inactive: { displayText: 'Inactive', color: 'default' },
  error: { displayText: 'Error', color: 'error' },
};

/**
 * Formats substatus, statusReason, and quarantineReason into a human-readable tooltip text.
 */
function formatSubstatusTooltipText({
  substatus,
  statusReason,
  quarantineReason,
}: {
  substatus?: BackendFeedStatus;
  statusReason?: BackendFeedStatusReason;
  quarantineReason?: string;
}): string {
  const parts: string[] = [];

  if (substatus) {
    const substatusDisplay =
      FEED_SUBSTATUS_UI_TEXT_DISPLAY[substatus] ?? substatus;
    const reasonDisplay = statusReason
      ? ` (${FEED_STATUS_REASON_UI_TEXT_DISPLAY[statusReason] ?? statusReason})`
      : '';
    parts.push(`${substatusDisplay}${reasonDisplay}`);
  } else if (statusReason) {
    parts.push(
      `(${FEED_STATUS_REASON_UI_TEXT_DISPLAY[statusReason] ?? statusReason})`
    );
  }

  if (quarantineReason) {
    parts.push(quarantineReason);
  }

  return parts.join(': ');
}

export function FeedStatusIndicator({
  status,
  substatus,
  statusReason,
  quarantineReason,
  lastHeartbeat,
  lastSpeechSegmentTimestamp,
}: {
  status?: FeedStatus;
  substatus?: BackendFeedStatus;
  statusReason?: BackendFeedStatusReason;
  quarantineReason?: string;
  lastHeartbeat?: number;
  lastSpeechSegmentTimestamp?: number;
}) {
  if (!status) {
    return null;
  }

  const statusConfig = FEED_STATUS_UI_CONFIG[status] ?? {
    displayText: status,
    color: 'default',
  };

  const substatusText = formatSubstatusTooltipText({
    substatus,
    statusReason,
    quarantineReason,
  });

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
        gap: 0.5,
        minWidth: 0,
        overflow: 'hidden',
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          minWidth: 0,
        }}
      >
        <Tooltip title={substatusText}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
            }}
          >
            <Badge
              color={statusConfig.color}
              variant="dot"
              sx={{
                py: 0,
                px: 0.5,
                display: 'flex',
                alignItems: 'center',
                flexShrink: 0,
              }}
            />
            <Typography
              variant="body2"
              sx={{
                color:
                  statusConfig.color === 'default'
                    ? 'text.secondary'
                    : `${statusConfig.color}.main`,
                fontWeight: 600,
                textTransform: 'uppercase',
                flexShrink: 0,
              }}
            >
              {statusConfig.displayText}
            </Typography>
          </Box>
        </Tooltip>
        <RelativeTimeDisplay
          label="Latest"
          timestamp={lastSpeechSegmentTimestamp}
        />
      </Box>
      {lastHeartbeat && (
        <RelativeTimeDisplay label="Last heartbeat" timestamp={lastHeartbeat} />
      )}
    </Box>
  );
}

export default FeedStatusIndicator;
