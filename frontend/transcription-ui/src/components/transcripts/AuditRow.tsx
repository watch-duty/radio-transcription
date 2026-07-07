import AddCircleIcon from '@mui/icons-material/AddCircle';
import BlockIcon from '@mui/icons-material/Block';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DeleteIcon from '@mui/icons-material/Delete';
import SettingsBackupRestoreIcon from '@mui/icons-material/SettingsBackupRestore';
import UpdateIcon from '@mui/icons-material/Update';
import WarningIcon from '@mui/icons-material/Warning';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import ListItem from '@mui/material/ListItem';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import type { FeedHistoryEvent, Tag } from '@transcription/common';

export function AuditRow({ auditEvent }: { auditEvent: FeedHistoryEvent }) {
  const theme = useTheme();
  const date = new Date(auditEvent.occurredAt);

  const getActionDetails = () => {
    const beforeStatus = auditEvent.beforeValues?.status as string | undefined;
    const afterStatus = auditEvent.afterValues?.status as string | undefined;

    let icon = <UpdateIcon fontSize="small" />;
    let message = '';

    switch (auditEvent.action) {
      case 'feed.created':
        icon = <AddCircleIcon fontSize="small" color="success" />;
        message = 'Feed created';
        break;
      case 'feed.deleted':
        icon = <DeleteIcon fontSize="small" color="error" />;
        message = 'Feed deleted';
        break;
      case 'feed.reset':
        icon = <SettingsBackupRestoreIcon fontSize="small" color="info" />;
        message = 'Feed reset by administrator';
        break;
      case 'feed.deactivated':
        icon = <BlockIcon fontSize="small" color="disabled" />;
        message = 'Feed deactivated by administrator';
        break;
      case 'feed.failure_reported':
        icon = <WarningIcon fontSize="small" color="error" />;
        message = 'Failure reported';
        break;
      case 'feed.quarantined':
        icon = <WarningIcon fontSize="small" color="error" />;
        message = 'Feed quarantined due to repeated failures';
        break;
      case 'feed.recovered':
        icon = <CheckCircleIcon fontSize="small" color="success" />;
        message = 'Feed recovered successfully';
        break;
      case 'feed.updated':
      default:
        message = 'Feed configuration updated';
        break;
    }

    if (beforeStatus && afterStatus && beforeStatus !== afterStatus) {
      const formatStatus = (s: string) =>
        s.charAt(0).toUpperCase() + s.slice(1);
      message += ` (status changed from ${formatStatus(beforeStatus)} to ${formatStatus(afterStatus)})`;
    }

    return { icon, message };
  };

  const { icon, message } = getActionDetails();

  const getStatusChipColor = (status: string) => {
    switch (status) {
      case 'active':
        return 'success';
      case 'failing':
      case 'quarantined':
        return 'error';
      case 'deactivated':
      case 'unclaimed':
      default:
        return 'default';
    }
  };

  const beforeStatus = auditEvent.beforeValues?.status as string | undefined;
  const afterStatus = auditEvent.afterValues?.status as string | undefined;

  const diffChanges =
    auditEvent.action === 'feed.updated'
      ? formatDiff(auditEvent.beforeValues, auditEvent.afterValues)
      : [];

  return (
    <ListItem
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        py: 1.5,
        px: 2,
        bgcolor: theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
        borderLeft: `5px solid ${theme.palette.divider}`,
      }}
    >
      <Box
        sx={{
          width: theme.spacing(3),
          display: 'flex',
          justifyContent: 'center',
          flexShrink: 0,
        }}
      >
        {icon}
      </Box>
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-end',
          width: 90,
          flexShrink: 0,
        }}
      >
        <Typography variant="caption" color="text.secondary">
          {date.toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            timeZoneName: 'short',
            hour12: false,
          })}
        </Typography>
      </Box>
      <Box
        sx={{
          flexGrow: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'flex-start',
          gap: 0.5,
        }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            flexWrap: 'wrap',
            width: '100%',
          }}
        >
          <Typography
            variant="body2"
            sx={{ fontWeight: 500, color: 'text.secondary' }}
          >
            [System Event]
          </Typography>
          <Typography
            variant="body2"
            color="text.primary"
            sx={{ fontStyle: 'italic' }}
          >
            {[
              'feed.reset',
              'feed.deactivated',
              'feed.created',
              'feed.deleted',
              'feed.updated',
            ].includes(auditEvent.action)
              ? `${message} by ${auditEvent.actor}`
              : message}
          </Typography>
          {beforeStatus && afterStatus && beforeStatus !== afterStatus && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 1 }}>
              <Chip
                label={beforeStatus.toUpperCase()}
                size="small"
                color={getStatusChipColor(beforeStatus)}
                variant="outlined"
              />
              <Typography variant="caption" color="text.secondary">
                →
              </Typography>
              <Chip
                label={afterStatus.toUpperCase()}
                size="small"
                color={getStatusChipColor(afterStatus)}
              />
            </Box>
          )}
        </Box>
        {diffChanges.length > 0 && (
          <Box
            sx={{
              pl: 2,
              display: 'flex',
              flexDirection: 'column',
              gap: 0.25,
            }}
          >
            {diffChanges.map((change, idx) => (
              <Typography key={idx} variant="caption" color="text.secondary">
                • {change}
              </Typography>
            ))}
          </Box>
        )}
      </Box>
    </ListItem>
  );
}

function getFieldLabel(key: string): string {
  switch (key) {
    case 'name':
      return 'Name';
    case 'status':
      return 'Status';
    case 'substatus':
      return 'Substatus';
    case 'sourceFeedId':
      return 'Source Feed ID';
    case 'sourceType':
      return 'Source Type';
    case 'statusReason':
      return 'Status Reason';
    case 'statusReasonDetail':
      return 'Status Reason Detail';
    case 'sourceUrl':
      return 'Source URL';
    case 'archiveUrl':
      return 'Archive URL';
    default:
      return key
        .replace(/([A-Z])/g, ' $1')
        .replace(/^./, (str) => str.toUpperCase());
  }
}

function formatDiff(
  before: Record<string, unknown> = {},
  after: Record<string, unknown> = {}
): string[] {
  const changes: string[] = [];
  const keys = Array.from(
    new Set([...Object.keys(before), ...Object.keys(after)])
  );

  keys.forEach((key) => {
    // Ignore status since it is highlighted via status chips
    if (key === 'status') {
      return;
    }

    const beforeVal = before[key];
    const afterVal = after[key];

    // Skip if values are equal
    if (JSON.stringify(beforeVal) === JSON.stringify(afterVal)) {
      return;
    }

    const fieldLabel = getFieldLabel(key);

    if (key === 'tags') {
      const beforeTags = (beforeVal as Tag[]) || [];
      const afterTags = (afterVal as Tag[]) || [];
      const beforeStrSet = new Set(
        beforeTags.map((t) => `${t.key}=${t.value}`)
      );
      const afterStrSet = new Set(afterTags.map((t) => `${t.key}=${t.value}`));

      const added = afterTags
        .filter((t) => !beforeStrSet.has(`${t.key}=${t.value}`))
        .map((t) => `"${t.key}=${t.value}"`);
      const removed = beforeTags
        .filter((t) => !afterStrSet.has(`${t.key}=${t.value}`))
        .map((t) => `"${t.key}=${t.value}"`);

      const tagChanges: string[] = [];
      if (added.length > 0) {
        tagChanges.push(`added ${added.join(', ')}`);
      }
      if (removed.length > 0) {
        tagChanges.push(`removed ${removed.join(', ')}`);
      }
      if (tagChanges.length > 0) {
        changes.push(`Tags: ${tagChanges.join(' and ')}`);
      }
      return;
    }

    if (beforeVal === undefined || beforeVal === null) {
      changes.push(`${fieldLabel} set to "${afterVal}"`);
    } else if (afterVal === undefined || afterVal === null) {
      changes.push(`${fieldLabel} cleared (was "${beforeVal}")`);
    } else {
      changes.push(
        `${fieldLabel} changed from "${beforeVal}" to "${afterVal}"`
      );
    }
  });

  return changes;
}
