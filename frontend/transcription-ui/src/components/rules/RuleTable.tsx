import { forwardRef, useCallback, useMemo, useState } from 'react';
import type { ComponentProps, HTMLAttributes } from 'react';
import { TableVirtuoso } from 'react-virtuoso';

import ClearIcon from '@mui/icons-material/Clear';
import RuleIcon from '@mui/icons-material/Rule';
import SearchIcon from '@mui/icons-material/Search';
import TuneIcon from '@mui/icons-material/Tune';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import Stack from '@mui/material/Stack';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import TableSortLabel from '@mui/material/TableSortLabel';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type { Feed, Rule, RuleConditions } from '@transcription/common';

import { useIsNarrow } from '../../hooks/useIsNarrow';
import { MultiSelectFilter } from '../common/MultiSelectFilter';
import { RuleRow } from './RuleRow';

export interface RuleTableProps {
  title?: string;
  rules: Rule[];
  feeds: Feed[];
  isLoading: boolean;
  allowEdit?: boolean;
  editingRuleId?: string;
  onEditRule?: (rule: Rule) => void;
  isSubmitting?: boolean;
}

interface SortConfig {
  column: 'name' | 'scope' | 'status';
  direction: 'asc' | 'desc';
}

const VirtuosoScroller = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => <TableContainer {...props} ref={ref} component="div" />);
VirtuosoScroller.displayName = 'VirtuosoScroller';

const VirtuosoTableHead = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <TableHead {...props} ref={ref} component="div" sx={{ display: 'block' }} />
));
VirtuosoTableHead.displayName = 'VirtuosoTableHead';

const VirtuosoTableBody = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <TableBody {...props} ref={ref} component="div" sx={{ display: 'block' }} />
));
VirtuosoTableBody.displayName = 'VirtuosoTableBody';

const VirtuosoFillerRow = ({ height }: { height: number }) => (
  <div style={{ height }} />
);
VirtuosoFillerRow.displayName = 'VirtuosoFillerRow';

const VirtuosoTable = forwardRef<HTMLDivElement, ComponentProps<typeof Table>>(
  (props, ref) => (
    <Table
      {...props}
      ref={ref}
      component="div"
      sx={{ display: 'block', width: '100%' }}
    />
  )
);
VirtuosoTable.displayName = 'VirtuosoTable';

function VirtuosoTableRow(
  props: ComponentProps<typeof TableRow> & {
    item?: Rule;
    context?: {
      editingRuleId?: string;
      allowEdit?: boolean;
      isNarrow?: boolean;
    };
  }
) {
  const { item, context, ...rest } = props;
  const isSelected = !!(item && context?.editingRuleId === item.ruleId);
  const allowEdit = context?.allowEdit ?? false;
  const isNarrow = context?.isNarrow ?? false;

  const gridTemplateColumns = isNarrow
    ? '1fr auto'
    : allowEdit
      ? '1.5fr 1fr 1.7fr 1fr 0.8fr 60px'
      : '1.5fr 1fr 1.7fr 1fr 0.8fr';

  return (
    <TableRow
      {...rest}
      component="div"
      hover
      selected={isSelected}
      sx={{
        display: 'grid',
        gridTemplateColumns,
        gridTemplateRows: isNarrow ? 'auto auto auto auto' : 'unset',
        gridTemplateAreas: isNarrow
          ? `
            "name-desc  actions"
            "scope      status"
            "conditions conditions"
            "tags       tags"
          `
          : 'unset',
        width: '100%',
        alignItems: 'center',
        borderBottom: '1px solid',
        borderColor: 'divider',
        borderLeft: '4px solid transparent',
        ...(isSelected && {
          bgcolor: 'action.selected',
          borderLeftColor: 'warning.main',
        }),
        ...rest.sx,
      }}
    />
  );
}

const VIRTUOSO_COMPONENTS = {
  Scroller: VirtuosoScroller,
  Table: VirtuosoTable,
  TableHead: VirtuosoTableHead,
  TableRow: VirtuosoTableRow,
  TableBody: VirtuosoTableBody,
  FillerRow: VirtuosoFillerRow,
};

export function RuleTable({
  title = 'Rules',
  rules,
  feeds,
  isLoading,
  allowEdit = false,
  editingRuleId,
  onEditRule,
  isSubmitting = false,
}: RuleTableProps) {
  const isNarrow = useIsNarrow();

  const [searchQuery, setSearchQuery] = useState('');
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    column: 'name',
    direction: 'asc',
  });

  const [appliedScopes, setAppliedScopes] = useState<string[]>([]);
  const [appliedStatuses, setAppliedStatuses] = useState<string[]>([]);

  const feedMap = useMemo(() => {
    return new Map<string, Feed>(feeds.map((f) => [f.id, f]));
  }, [feeds]);

  const ruleMap = useMemo(() => {
    return new Map<string, Rule>(rules.map((r) => [r.ruleId, r]));
  }, [rules]);

  const handleRequestSort = (property: 'name' | 'scope' | 'status') => {
    setSortConfig((prev) => ({
      column: property,
      direction:
        prev.column === property && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  const getConditionsSearchText = useCallback(
    (conditions: RuleConditions): string => {
      if (conditions.evaluationType === 'KEYWORD_MATCH') {
        const caseStr = conditions.caseSensitive ? ' (case-sensitive)' : '';
        return `Keywords (${conditions.operator})${caseStr}: ${conditions.keywords.join(', ')}`;
      }
      if (conditions.evaluationType === 'REGEX_MATCH') {
        const flagsStr = conditions.flags ? ` / ${conditions.flags}` : '';
        return `Regex: /${conditions.expression}${flagsStr}`;
      }
      if (conditions.evaluationType === 'RULE_GROUP') {
        const childNames = conditions.childRuleIds.map(
          (id) => ruleMap.get(id)?.ruleName || id
        );
        return `Rule Group (${conditions.operator}): ${childNames.join(', ')}`;
      }
      return '';
    },
    [ruleMap]
  );

  const filteredAndSortedRules = useMemo(() => {
    const query = searchQuery.toLowerCase().trim();
    let filtered = rules;

    // 1. Text search filtering
    if (query) {
      filtered = filtered.filter((rule) => {
        const nameMatches = rule.ruleName.toLowerCase().includes(query);
        const descMatches =
          rule.description?.toLowerCase().includes(query) ?? false;
        const conditionMatches = getConditionsSearchText(rule.conditions)
          .toLowerCase()
          .includes(query);
        return nameMatches || descMatches || conditionMatches;
      });
    }

    // 2. Scope filtering
    if (appliedScopes.length > 0) {
      filtered = filtered.filter((rule) =>
        appliedScopes.includes(rule.scope.level)
      );
    }

    // 3. Status filtering
    if (appliedStatuses.length > 0) {
      filtered = filtered.filter((rule) => {
        const statusStr = rule.isActive ? 'Active' : 'Inactive';
        return appliedStatuses.includes(statusStr);
      });
    }

    // 4. Sorting
    return filtered.sort((a, b) => {
      let comparison = 0;
      if (sortConfig.column === 'name') {
        comparison = a.ruleName.localeCompare(b.ruleName);
      } else if (sortConfig.column === 'scope') {
        comparison = a.scope.level.localeCompare(b.scope.level);
      } else if (sortConfig.column === 'status') {
        comparison = (a.isActive ? 1 : 0) - (b.isActive ? 1 : 0);
      }
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
  }, [
    rules,
    searchQuery,
    appliedScopes,
    appliedStatuses,
    sortConfig,
    getConditionsSearchText,
  ]);

  const gridTemplateColumns = allowEdit
    ? '1.5fr 1fr 1.7fr 1fr 0.8fr 60px'
    : '1.5fr 1fr 1.7fr 1fr 0.8fr';

  const columns = [
    { key: 'name', display: 'Rule Name' },
    { key: 'scope', display: 'Scope' },
    { key: 'conditions', display: 'Conditions' },
    { key: 'tags', display: 'Tags' },
    { key: 'status', display: 'Status' },
  ];

  const tableHeader = (
    <TableRow
      component="div"
      sx={{
        display: 'grid',
        gridTemplateColumns,
        width: '100%',
      }}
    >
      {columns.map(({ key, display }) => (
        <TableCell
          key={key}
          component="div"
          role="columnheader"
          sx={{
            fontWeight: 'bold',
            bgcolor: 'background.paper',
          }}
        >
          {key === 'conditions' || key === 'tags' ? (
            display
          ) : (
            <TableSortLabel
              active={sortConfig.column === key}
              direction={
                sortConfig.column === key ? sortConfig.direction : 'asc'
              }
              onClick={() =>
                handleRequestSort(key as 'name' | 'scope' | 'status')
              }
            >
              {display}
            </TableSortLabel>
          )}
        </TableCell>
      ))}
      {allowEdit ? (
        <TableCell
          component="div"
          role="columnheader"
          align="right"
          sx={{
            fontWeight: 'bold',
            bgcolor: 'background.paper',
          }}
        />
      ) : null}
    </TableRow>
  );

  return (
    <Card
      variant="outlined"
      data-testid="rules-deck-card"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: { xs: 'auto', sm: 0 },
        overflow: 'hidden',
        borderRadius: 2,
        boxShadow: '0 4px 20px rgba(0,0,0,0.05)',
      }}
    >
      <Box
        sx={{
          p: 2,
          display: 'flex',
          flexDirection: 'column',
          gap: 2,
          bgcolor: 'background.paper',
        }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: 1.5,
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <RuleIcon color="primary" fontSize="small" />
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              {title}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            {filteredAndSortedRules.length !== rules.length && (
              <Typography variant="body2" color="text.secondary">
                Showing {filteredAndSortedRules.length} of {rules.length} rules
              </Typography>
            )}
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{ fontWeight: 500 }}
            >
              {rules.length} Rules
            </Typography>
          </Box>
        </Box>

        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: 1.5,
            width: '100%',
          }}
        >
          <TextField
            fullWidth
            size="small"
            placeholder="Search rules..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon color="action" fontSize="small" />
                  </InputAdornment>
                ),
                endAdornment: searchQuery ? (
                  <InputAdornment position="end">
                    <IconButton size="small" onClick={() => setSearchQuery('')}>
                      <ClearIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                ) : null,
              },
            }}
            sx={{
              flexGrow: 1,
              minWidth: { xs: '100%', md: 200 },
              maxWidth: { md: 400 },
            }}
          />

          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1.5,
              flexGrow: 1,
              flexWrap: 'wrap',
              width: { xs: '100%', sm: 'auto' },
            }}
          >
            <TuneIcon color="action" fontSize="small" />
            <Box sx={{ flexGrow: 1 }}>
              <MultiSelectFilter
                label="Scope"
                options={['GLOBAL', 'FEED_SPECIFIC']}
                value={appliedScopes}
                onChange={setAppliedScopes}
                size="small"
              />
            </Box>
            <Box sx={{ flexGrow: 1 }}>
              <MultiSelectFilter
                label="Status"
                options={['Active', 'Inactive']}
                value={appliedStatuses}
                onChange={setAppliedStatuses}
                size="small"
              />
            </Box>
          </Box>
        </Box>
      </Box>

      <Divider />

      {isLoading ? (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            flexGrow: 1,
            py: 6,
          }}
        >
          <Stack spacing={2} sx={{ alignItems: 'center' }}>
            <CircularProgress size={36} thickness={4} />
            <Typography variant="body2" color="text.secondary">
              Loading rules...
            </Typography>
          </Stack>
        </Box>
      ) : filteredAndSortedRules.length === 0 ? (
        <Box
          sx={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            flexGrow: 1,
            py: 6,
            textAlign: 'center',
            px: 3,
          }}
        >
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ fontWeight: 500 }}
          >
            {searchQuery
              ? 'No rules matching filter query found.'
              : 'No rule found.'}
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
            {searchQuery
              ? 'Refine spelling or delete terms to widen search scope.'
              : 'Create rules on the left to start processing.'}
          </Typography>
        </Box>
      ) : isNarrow ? (
        <TableContainer
          component="div"
          sx={{ flexGrow: 1, overflowY: 'visible' }}
        >
          <Table component="div" sx={{ display: 'block', width: '100%' }}>
            <TableBody component="div" sx={{ display: 'block' }}>
              {filteredAndSortedRules.map((rule) => (
                <VirtuosoTableRow
                  key={rule.ruleId}
                  item={rule}
                  context={{ editingRuleId, allowEdit, isNarrow: true }}
                >
                  <RuleRow
                    rule={rule}
                    feedMap={feedMap}
                    ruleMap={ruleMap}
                    editingRuleId={editingRuleId}
                    allowEdit={allowEdit}
                    onEditRule={onEditRule}
                    isSubmitting={isSubmitting}
                    isNarrow={true}
                  />
                </VirtuosoTableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      ) : (
        <TableVirtuoso
          data={filteredAndSortedRules}
          context={{ editingRuleId, allowEdit, isNarrow: false }}
          computeItemKey={(_index, rule) => rule.ruleId}
          components={VIRTUOSO_COMPONENTS}
          style={{ flexGrow: 1, minHeight: 0 }}
          fixedHeaderContent={() => tableHeader}
          itemContent={(_index, rule) => (
            <RuleRow
              rule={rule}
              feedMap={feedMap}
              ruleMap={ruleMap}
              editingRuleId={editingRuleId}
              allowEdit={allowEdit}
              onEditRule={onEditRule}
              isSubmitting={isSubmitting}
              isNarrow={false}
            />
          )}
        />
      )}
    </Card>
  );
}

export default RuleTable;
