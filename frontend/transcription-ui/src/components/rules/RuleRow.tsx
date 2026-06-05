import type { ReactNode } from 'react';

import EditIcon from '@mui/icons-material/Edit';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import TableCell from '@mui/material/TableCell';
import Typography from '@mui/material/Typography';
import type { Feed, Rule } from '@transcription/common';

export interface RuleRowProps {
  rule: Rule;
  feedMap: Map<string, Feed>;
  ruleMap: Map<string, Rule>;
  editingRuleId?: string;
  allowEdit: boolean;
  onEditRule?: (rule: Rule) => void;
  isSubmitting?: boolean;
}

export function RuleRow({
  rule,
  feedMap,
  ruleMap,
  editingRuleId,
  allowEdit,
  onEditRule,
  isSubmitting = false,
}: RuleRowProps) {
  const isEditing = editingRuleId === rule.ruleId;
  const targetFeedNames = rule.scope.targetFeeds
    .map((id) => feedMap.get(id)?.name || id)
    .join(', ');

  const renderConditions = (): ReactNode => {
    const { conditions } = rule;
    if (conditions.evaluationType === 'KEYWORD_MATCH') {
      const caseStr = conditions.caseSensitive ? ' (case-sensitive)' : '';
      return (
        <Box
          sx={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 0.5,
          }}
        >
          <Typography variant="body2" color="text.secondary" sx={{ mr: 0.5 }}>
            Keywords ({conditions.operator}){caseStr}:
          </Typography>
          {conditions.keywords.map((kw, idx) => (
            <Chip key={idx} label={kw} size="small" />
          ))}
        </Box>
      );
    }
    if (conditions.evaluationType === 'REGEX_MATCH') {
      const label = `/${conditions.expression}/${conditions.flags || ''}`;
      return (
        <Box
          sx={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 0.5,
          }}
        >
          <Typography variant="body2" color="text.secondary" sx={{ mr: 0.5 }}>
            Regex:
          </Typography>
          <Chip label={label} size="small" sx={{ fontFamily: 'monospace' }} />
        </Box>
      );
    }
    if (conditions.evaluationType === 'RULE_GROUP') {
      return (
        <Box
          sx={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 0.5,
          }}
        >
          <Typography variant="body2" color="text.secondary" sx={{ mr: 0.5 }}>
            Rule Group ({conditions.operator}):
          </Typography>
          {conditions.childRuleIds.map((id) => {
            const childName = ruleMap.get(id)?.ruleName || id;
            return <Chip key={id} label={childName} size="small" />;
          })}
        </Box>
      );
    }
    return null;
  };

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
        {renderConditions()}
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
