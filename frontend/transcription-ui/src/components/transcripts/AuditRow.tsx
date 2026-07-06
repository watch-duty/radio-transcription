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
import type { FeedHistoryEvent } from '@transcription/common';

export function AuditRow({ auditEvent }: { auditEvent: FeedHistoryEvent }) {
  const theme = useTheme();
  const date = new Date(auditEvent.occurredAt);

  const getActionDetails = () => {
    const beforeStatus = auditEvent.beforeValues?.status;
    const afterStatus = auditEvent.afterValues?.status;

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

  const beforeStatus = auditEvent.beforeValues?.status;
  const afterStatus = auditEvent.afterValues?.status;

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
          alignItems: 'center',
          gap: 1,
          flexWrap: 'wrap',
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
    </ListItem>
  );
}
