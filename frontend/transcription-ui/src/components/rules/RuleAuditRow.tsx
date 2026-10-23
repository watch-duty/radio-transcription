import React from 'react';

import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import ListItem from '@mui/material/ListItem';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { RuleAuditEvent } from '@transcription/common';

import { formatDiff } from './RuleAuditRow.utils';

interface RuleAuditRowProps {
  auditEvent: RuleAuditEvent;
}

export function RuleAuditRow({ auditEvent }: RuleAuditRowProps) {
  const diffs = formatDiff(auditEvent.beforeValues, auditEvent.afterValues);

  const timeStr = new Date(auditEvent.occurredAt).toLocaleTimeString([], {
    hour: 'numeric',
    minute: '2-digit',
  });

  return (
    <ListItem
      divider
      sx={{
        display: 'flex',
        alignItems: 'flex-start',
        py: 2,
        px: 3,
      }}
    >
      <Box sx={{ minWidth: 100, pt: 0.5, mr: 2 }}>
        <Typography variant="body2" color="text.secondary">
          {timeStr}
        </Typography>
      </Box>

      <Box sx={{ flex: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 1, gap: 1 }}>
          <Chip
            label={auditEvent.action}
            size="small"
            color={
              auditEvent.action === 'rule.deleted'
                ? 'error'
                : auditEvent.action === 'rule.created'
                  ? 'success'
                  : 'primary'
            }
          />
          <Tooltip title={`Actor ID: ${auditEvent.actorId}`}>
            <Typography variant="body2" sx={{ fontWeight: 'medium' }}>
              by {auditEvent.actorId.split('@')[0]}
            </Typography>
          </Tooltip>
        </Box>

        {diffs.length > 0 ? (
          <Box
            component="ul"
            sx={{
              m: 0,
              pl: 2,
              '& li': {
                mb: 0.5,
                wordBreak: 'break-word',
              },
            }}
          >
            {diffs.map((diff, idx) => (
              <Typography component="li" variant="body2" key={idx}>
                {diff}
              </Typography>
            ))}
          </Box>
        ) : (
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            No meaningful changes.
          </Typography>
        )}
      </Box>
    </ListItem>
  );
}
