import EditIcon from '@mui/icons-material/Edit';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import TableCell from '@mui/material/TableCell';
import Typography from '@mui/material/Typography';
import type { Feed, Rule, RuleConditions } from '@transcription/common';

export interface RuleRowProps {
  rule: Rule;
  feedMap: Map<string, Feed>;
  editingRuleId?: string;
  allowEdit: boolean;
  onEditRule?: (rule: Rule) => void;
  isSubmitting?: boolean;
  formatConditionsSummary: (conditions: RuleConditions) => string;
}

export function RuleRow({
  rule,
  feedMap,
  editingRuleId,
  allowEdit,
  onEditRule,
  isSubmitting = false,
  formatConditionsSummary,
}: RuleRowProps) {
  const isEditing = editingRuleId === rule.ruleId;
  const targetFeedNames = rule.scope.targetFeeds
    .map((id) => feedMap.get(id)?.name || id)
    .join(', ');

  return (
    <>
      <TableCell
        component="div"
        role="cell"
        sx={{
          py: 1,
          display: 'flex',
          flexDirection: 'column',
          borderBottom: 'none',
          minWidth: 0,
          alignItems: 'flex-start',
        }}
      >
        <Typography
          variant="body2"
          sx={{
            fontWeight: 600,
            color: 'text.primary',
            textOverflow: 'ellipsis',
            maxWidth: '100%',
          }}
          noWrap
        >
          {rule.ruleName}
        </Typography>
        {rule.description ? (
          <Typography
            variant="caption"
            color="text.secondary"
            noWrap
            sx={{ maxWidth: '100%' }}
          >
            {rule.description}
          </Typography>
        ) : null}
      </TableCell>

      <TableCell
        component="div"
        role="cell"
        sx={{
          py: 1,
          display: 'flex',
          flexDirection: 'column',
          borderBottom: 'none',
          minWidth: 0,
          alignItems: 'flex-start',
        }}
      >
        <Chip label={rule.scope.level} size="small" variant="outlined" />
        {rule.scope.level === 'FEED_SPECIFIC' &&
        rule.scope.targetFeeds.length > 0 ? (
          <Typography
            variant="caption"
            color="text.secondary"
            noWrap
            sx={{ maxWidth: '100%', mt: 0.5 }}
            title={targetFeedNames}
          >
            Feeds: {targetFeedNames}
          </Typography>
        ) : null}
      </TableCell>

      <TableCell
        component="div"
        role="cell"
        sx={{
          py: 1,
          borderBottom: 'none',
          minWidth: 0,
        }}
      >
        <Typography
          variant="body2"
          sx={{ fontFamily: 'monospace', wordBreak: 'break-all' }}
        >
          {formatConditionsSummary(rule.conditions)}
        </Typography>
      </TableCell>

      <TableCell
        component="div"
        role="cell"
        sx={{ borderBottom: 'none', minWidth: 0 }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 1,
          }}
        >
          <Badge
            color={rule.isActive ? 'success' : 'default'}
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
              color: rule.isActive ? 'success.main' : 'text.secondary',
              fontWeight: 600,
              textTransform: 'uppercase',
            }}
          >
            {rule.isActive ? 'Active' : 'Inactive'}
          </Typography>
        </Box>
      </TableCell>

      {allowEdit ? (
        <TableCell
          align="right"
          component="div"
          role="cell"
          sx={{
            borderBottom: 'none',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-end',
            minWidth: 0,
          }}
        >
          <IconButton
            size="small"
            onClick={() => onEditRule?.(rule)}
            disabled={isSubmitting || isEditing}
            sx={{
              border: '1px solid',
              borderColor: 'divider',
              borderRadius: 1.5,
              p: 0.5,
              '&:hover': {
                borderColor: 'primary.main',
                bgcolor: 'primary.soft',
                color: 'primary.main',
              },
            }}
            aria-label={`Edit ${rule.ruleName}`}
          >
            <EditIcon fontSize="small" />
          </IconButton>
        </TableCell>
      ) : null}
    </>
  );
}
