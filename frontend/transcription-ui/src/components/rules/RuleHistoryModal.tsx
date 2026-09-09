import React from 'react';

import CloseIcon from '@mui/icons-material/Close';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import List from '@mui/material/List';
import ListSubheader from '@mui/material/ListSubheader';
import Typography from '@mui/material/Typography';
import { useInfiniteQuery } from '@tanstack/react-query';
import type { Rule } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listRuleHistory } from '../../service/listRuleHistory';
import { RuleAuditRow } from './RuleAuditRow';
import { groupEventsByDate } from './RuleAuditRow.utils';

interface RuleHistoryModalProps {
  rule: Rule | null;
  open: boolean;
  onClose: () => void;
}

export function RuleHistoryModal({
  rule,
  open,
  onClose,
}: RuleHistoryModalProps) {
  const { token } = useAuth();

  const {
    data,
    isLoading,
    error,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['listRuleHistory', token, rule?.ruleId],
    queryFn: ({ pageParam }) =>
      listRuleHistory(rule!.ruleId, token!, 50, pageParam),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.nextToken,
    enabled: open && !!token && !!rule?.ruleId,
  });

  const historyEvents = data?.pages.flatMap((page) => page.auditEvents) ?? [];

  return (
    <Dialog
      open={open}
      onClose={onClose}
      fullWidth
      maxWidth="md"
      aria-labelledby="rule-history-dialog-title"
    >
      <DialogTitle id="rule-history-dialog-title" sx={{ m: 0, p: 2, pr: 6 }}>
        {rule ? `Audit Trail: ${rule.ruleName}` : 'Audit Trail'}
        <IconButton
          aria-label="close"
          onClick={onClose}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8,
            color: (theme) => theme.palette.grey[500],
          }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent dividers sx={{ p: 0 }}>
        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Box sx={{ p: 3 }}>
            <Typography color="error" align="center">
              Error loading audit trail
            </Typography>
          </Box>
        ) : historyEvents.length === 0 ? (
          <Box sx={{ p: 3 }}>
            <Typography color="text.secondary" align="center">
              No audit events found for this rule.
            </Typography>
          </Box>
        ) : (
          <Box
            sx={{
              maxHeight: '60vh',
              overflowY: 'auto',
            }}
          >
            <List
              sx={{
                width: '100%',
                py: 0,
              }}
            >
              {groupEventsByDate(historyEvents).map((group) => (
                <React.Fragment key={group.dateStr}>
                  <ListSubheader
                    sx={{
                      bgcolor: 'background.paper',
                      fontWeight: 'bold',
                      lineHeight: '36px',
                    }}
                  >
                    {group.dateStr}
                  </ListSubheader>
                  {group.events.map((event) => (
                    <RuleAuditRow key={event.id} auditEvent={event} />
                  ))}
                </React.Fragment>
              ))}
            </List>
            {hasNextPage && (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
                <Button
                  onClick={() => fetchNextPage()}
                  disabled={isFetchingNextPage}
                  variant="outlined"
                  size="small"
                >
                  {isFetchingNextPage ? 'Loading more...' : 'Load More'}
                </Button>
              </Box>
            )}
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ p: 2 }}>
        <Button onClick={onClose} variant="outlined">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
}
