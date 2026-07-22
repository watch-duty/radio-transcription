import { forwardRef, useMemo, useState } from 'react';
import type { ComponentProps, HTMLAttributes } from 'react';
import { Link as RouterLink } from 'react-router';
import { TableVirtuoso } from 'react-virtuoso';

import pluralize from 'pluralize';

import ClearIcon from '@mui/icons-material/Clear';
import EditIcon from '@mui/icons-material/Edit';
import SearchIcon from '@mui/icons-material/Search';
import TroubleshootIcon from '@mui/icons-material/Troubleshoot';
import TuneIcon from '@mui/icons-material/Tune';
import VisibilityIcon from '@mui/icons-material/Visibility';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import Link from '@mui/material/Link';
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
import { type Feed, SourceType } from '@transcription/common';

import { useIsNarrow } from '../../hooks/useIsNarrow';
import { toSourceTypeString } from '../../utils/textUtils';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { MultiSelectFilter } from '../common/MultiSelectFilter';
import { FeedHistoryModal } from './FeedHistoryModal';
import { FeedTagChip } from './FeedTagChip';
import { groupTagsByKey } from './tagDisplay';

export interface FeedFilters {
  searchQuery: string;
  sourceTypes: SourceType[];
  statuses: string[];
  tags: { key: string; value: string }[];
}

export interface FeedTableProps {
  title?: string;
  feeds: Feed[];
  tags: { key: string; value: string }[];
  isLoading: boolean;
  feedTotal: number;
  allowEdit?: boolean;
  editingFeedId?: string;
  onEditFeed?: (feed: Feed) => void;
  isSubmitting?: boolean;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
}

interface SortConfig {
  column: 'name' | 'type' | 'status';
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
    item?: Feed;
    context?: {
      editingFeedId?: string;
      allowEdit?: boolean;
      isNarrow?: boolean;
    };
  }
) {
  const { item, context, ...rest } = props;
  const isSelected = !!(item && context?.editingFeedId === item.id);
  const allowEdit = context?.allowEdit ?? false;
  const isNarrow = context?.isNarrow ?? false;

  const gridTemplateColumns = isNarrow
    ? '1fr auto'
    : allowEdit
      ? '1.5fr 1fr 1fr 100px'
      : '1.5fr 1fr 1fr 2fr';

  return (
    <TableRow
      {...rest}
      component="div"
      hover
      selected={isSelected}
      sx={{
        display: 'grid',
        gridTemplateColumns,
        gridTemplateRows: isNarrow ? 'auto auto auto' : 'unset',
        gridTemplateAreas: isNarrow
          ? `
            "name-source status"
            "type        links-actions"
            "tags        tags"
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

const ALL_SOURCE_TYPES = Object.values(SourceType);

export function FeedTable({
  title = 'Feeds',
  feeds,
  tags,
  isLoading,
  feedTotal,
  allowEdit = false,
  editingFeedId,
  onEditFeed,
  isSubmitting = false,
  filters,
  onFiltersChange,
}: FeedTableProps) {
  const isNarrow = useIsNarrow();

  const [historyFeed, setHistoryFeed] = useState<Feed | null>(null);

  const [sortConfig, setSortConfig] = useState<SortConfig>({
    column: 'name',
    direction: 'asc',
  });

  const handleRequestSort = (property: 'name' | 'type' | 'status') => {
    setSortConfig((prev) => ({
      column: property,
      direction:
        prev.column === property && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  const sortFeeds = useMemo(() => {
    return [...feeds].sort((a, b) => {
      let comparison = 0;
      if (sortConfig.column === 'name') {
        comparison = a.name.localeCompare(b.name);
      } else if (sortConfig.column === 'type') {
        comparison = a.sourceType.localeCompare(b.sourceType);
      } else if (sortConfig.column === 'status') {
        comparison = a.status.localeCompare(b.status);
      }
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
  }, [feeds, sortConfig]);

  const gridTemplateColumns = allowEdit
    ? '1.5fr 1fr 1fr 100px'
    : '1.5fr 1fr 1fr 2fr';

  const sortConfigColumn = sortConfig.column;
  const columns = [
    { key: 'name', display: 'Name' },
    { key: 'type', display: 'Type' },
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
          <TableSortLabel
            active={sortConfigColumn === key}
            direction={sortConfigColumn === key ? sortConfig.direction : 'asc'}
            onClick={() => handleRequestSort(key as 'name' | 'type' | 'status')}
          >
            {display}
          </TableSortLabel>
        </TableCell>
      ))}
      <TableCell
        component="div"
        role="columnheader"
        align={allowEdit ? 'right' : 'left'}
        sx={{
          fontWeight: 'bold',
          display: 'flex',
          alignItems: 'center',
          bgcolor: 'background.paper',
        }}
      >
        {allowEdit ? '' : 'Links'}
      </TableCell>
    </TableRow>
  );

  const renderRowContent = (feed: Feed) => {
    const isEditing = editingFeedId === feed.id;

    return (
      <>
        <TableCell
          component="div"
          role="cell"
          sx={{
            gridArea: { xs: 'name-source', sm: 'unset' },
            py: 1,
            display: 'flex',
            flexDirection: 'column',
            borderBottom: 'none',
            minWidth: 0,
            alignItems: 'flex-start',
          }}
        >
          <Link
            component={RouterLink}
            to={`/transcripts?feedId=${feed.id}`}
            variant="body2"
            sx={{
              fontWeight: 600,
              textDecoration: 'none',
              color: 'primary.main',
              '&:hover': { textDecoration: 'underline' },
              textOverflow: 'ellipsis',
              maxWidth: '100%',
            }}
            noWrap
          >
            {feed.name}
          </Link>

          <Typography variant="caption" color="text.secondary">
            <b>Source ID:</b> {feed.sourceFeedId}
          </Typography>
        </TableCell>

        <TableCell
          component="div"
          role="cell"
          sx={{
            gridArea: { xs: 'type', sm: 'unset' },
            borderBottom: 'none',
            minWidth: 0,
            py: { xs: 0.5, sm: undefined },
          }}
        >
          <Chip
            label={toSourceTypeString(feed.sourceType)}
            size="small"
            variant="outlined"
          />
        </TableCell>

        <TableCell
          component="div"
          role="cell"
          sx={{
            gridArea: { xs: 'status', sm: 'unset' },
            borderBottom: 'none',
            minWidth: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: { xs: 'flex-end', sm: 'flex-start' },
            py: { xs: 0.5, sm: undefined },
          }}
        >
          <FeedStatusIndicator
            status={feed.status}
            substatus={feed.substatus}
            statusReason={feed.statusReason}
            statusReasonDetail={feed.statusReasonDetail}
            lastHeartbeat={allowEdit ? feed.lastHeartbeat : undefined}
            lastSpeechSegmentTimestamp={feed.lastSpeechSegmentTimestamp}
          />
        </TableCell>

        <TableCell
          align={allowEdit ? 'right' : 'left'}
          component="div"
          role="cell"
          sx={{
            gridArea: { xs: 'links-actions', sm: 'unset' },
            borderBottom: 'none',
            display: 'flex',
            alignItems: 'center',
            minWidth: 0,
            width: '100%',
            py: { xs: 0.5, sm: undefined },
            gap: 1,
            ...(allowEdit && {
              justifyContent: {
                xs: 'flex-end',
                sm: allowEdit ? 'flex-end' : 'flex-start',
              },
            }),
          }}
        >
          {allowEdit ? (
            <IconButton
              size="small"
              onClick={() => onEditFeed?.(feed)}
              disabled={isSubmitting || isEditing}
              sx={{
                border: '1px solid',
                borderRadius: 1.5,
                p: 0.5,
                '&:hover': {
                  borderColor: 'primary.main',
                  bgcolor: 'primary.soft',
                  color: 'primary.main',
                },
              }}
              aria-label={`Edit ${feed.name}`}
            >
              <EditIcon fontSize="small" />
            </IconButton>
          ) : (
            <Box
              sx={{
                display: 'flex',
                flexDirection: { xs: 'row', sm: 'column' },
                gap: { xs: 1.5, sm: 0.5 },
                alignItems: { xs: 'center', sm: 'flex-start' },
                flexGrow: 1,
                overflow: 'hidden',
                justifyContent: { xs: 'flex-end', sm: 'flex-start' },
              }}
            >
              {feed.sourceUrl ? (
                <Typography
                  variant="caption"
                  noWrap
                  sx={{ maxWidth: '100%', textOverflow: 'ellipsis' }}
                >
                  {isNarrow ? (
                    <Link
                      href={feed.sourceUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      underline="hover"
                      sx={{ color: 'primary.main', fontWeight: 600 }}
                    >
                      Source
                    </Link>
                  ) : (
                    <>
                      <b>Source:</b>{' '}
                      <Link
                        href={feed.sourceUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        underline="hover"
                        sx={{ color: 'primary.main' }}
                      >
                        {feed.sourceUrl}
                      </Link>
                    </>
                  )}
                </Typography>
              ) : null}
              {feed.archiveUrl ? (
                <Typography
                  variant="caption"
                  noWrap
                  sx={{ maxWidth: '100%', textOverflow: 'ellipsis' }}
                >
                  {isNarrow ? (
                    <Link
                      href={feed.archiveUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      underline="hover"
                      sx={{ color: 'primary.main', fontWeight: 600 }}
                    >
                      Archive
                    </Link>
                  ) : (
                    <>
                      <b>Archive:</b>{' '}
                      <Link
                        href={feed.archiveUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        underline="hover"
                        sx={{ color: 'primary.main' }}
                      >
                        {feed.archiveUrl}
                      </Link>
                    </>
                  )}
                </Typography>
              ) : null}
              {!feed.sourceUrl && !feed.archiveUrl ? (
                <Typography variant="caption" color="text.secondary">
                  -
                </Typography>
              ) : null}
            </Box>
          )}

          <IconButton
            size="small"
            onClick={() => setHistoryFeed(feed)}
            sx={{
              border: '1px solid',
              borderRadius: 1.5,
              p: 0.5,
              flexShrink: 0,
              '&:hover': {
                borderColor: 'primary.main',
                bgcolor: 'primary.soft',
                color: 'primary.main',
              },
            }}
            aria-label={`View audit trail for ${feed.name}`}
          >
            <VisibilityIcon fontSize="small" />
          </IconButton>
        </TableCell>

        {feed.tags && feed.tags.length > 0 ? (
          <TableCell
            component="div"
            role="cell"
            sx={{
              gridArea: { xs: 'tags', sm: 'unset' },
              gridColumn: { xs: 'unset', sm: '1 / -1' },
              borderBottom: 'none',
              pt: 0,
              pb: { xs: 1.5, sm: undefined },
              display: 'flex',
              flexWrap: 'wrap',
              gap: 0.75,
            }}
          >
            {groupTagsByKey(feed.tags).map((group) => (
              <FeedTagChip
                key={`feed-${feed.id}-tag-${group.key}`}
                group={group}
              />
            ))}
          </TableCell>
        ) : null}
      </>
    );
  };

  return (
    <Card
      variant="outlined"
      data-testid="feeds-deck-card"
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
            <TroubleshootIcon color="primary" fontSize="small" />
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              {title}
            </Typography>
          </Box>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ fontWeight: 500 }}
          >
            {!isLoading && `${feedTotal} ${pluralize('Feed', feedTotal)}`}
          </Typography>
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
            placeholder="Search feeds..."
            value={filters.searchQuery}
            onChange={(e) =>
              onFiltersChange({ ...filters, searchQuery: e.target.value })
            }
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon color="action" fontSize="small" />
                  </InputAdornment>
                ),
                endAdornment: filters.searchQuery ? (
                  <InputAdornment position="end">
                    <IconButton
                      size="small"
                      onClick={() =>
                        onFiltersChange({ ...filters, searchQuery: '' })
                      }
                    >
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
                label="Source Type"
                options={ALL_SOURCE_TYPES}
                value={filters.sourceTypes}
                onChange={(types) =>
                  onFiltersChange({ ...filters, sourceTypes: types })
                }
                getOptionLabel={toSourceTypeString}
                renderOptionContent={toSourceTypeString}
                renderValueLabel={toSourceTypeString}
                size="small"
              />
            </Box>
            <Box sx={{ flexGrow: 1 }}>
              <MultiSelectFilter
                label="Status"
                options={['Active', 'Inactive', 'Error']}
                value={filters.statuses}
                onChange={(statuses) =>
                  onFiltersChange({ ...filters, statuses })
                }
                size="small"
              />
            </Box>
            <Box sx={{ flexGrow: 1 }}>
              <MultiSelectFilter
                label="Tags"
                options={tags}
                value={filters.tags}
                onChange={(tags) => onFiltersChange({ ...filters, tags })}
                size="small"
                groupBy={(tag) => tag.key}
                getOptionLabel={(tag) => `${tag.key}: ${tag.value}`}
                getOptionValue={(tag) => `${tag.key}:${tag.value}`}
                isOptionEqualToValue={(a, b) =>
                  a.key === b.key && a.value === b.value
                }
                renderOptionContent={(tag) => tag.value}
                renderValueLabel={(tag) => (
                  <Typography variant="body2">
                    <b>{tag.key}</b>: {tag.value}
                  </Typography>
                )}
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
              Loading feeds...
            </Typography>
          </Stack>
        </Box>
      ) : sortFeeds.length === 0 ? (
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
            {filters.searchQuery
              ? 'No feeds matching filter query found.'
              : 'No feed found.'}
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
            {filters.searchQuery
              ? 'Refine spelling or delete terms to widen search scope.'
              : 'Register feeds on the left to start listening.'}
          </Typography>
        </Box>
      ) : isNarrow ? (
        <TableContainer
          component="div"
          sx={{ flexGrow: 1, overflowY: 'visible' }}
        >
          <Table component="div" sx={{ display: 'block', width: '100%' }}>
            <TableBody component="div" sx={{ display: 'block' }}>
              {sortFeeds.map((feed) => (
                <VirtuosoTableRow
                  key={feed.id}
                  item={feed}
                  context={{ editingFeedId, allowEdit, isNarrow: true }}
                >
                  {renderRowContent(feed)}
                </VirtuosoTableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      ) : (
        <TableVirtuoso
          data={sortFeeds}
          context={{ editingFeedId, allowEdit, isNarrow: false }}
          computeItemKey={(_index, feed) => feed.id}
          components={VIRTUOSO_COMPONENTS}
          style={{ flexGrow: 1, minHeight: 0 }}
          fixedHeaderContent={() => tableHeader}
          itemContent={(_index, feed) => renderRowContent(feed)}
        />
      )}
      {historyFeed && (
        <FeedHistoryModal
          feed={historyFeed}
          open={true}
          onClose={() => setHistoryFeed(null)}
        />
      )}
    </Card>
  );
}

export default FeedTable;
