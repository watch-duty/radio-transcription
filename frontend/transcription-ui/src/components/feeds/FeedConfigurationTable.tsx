import { forwardRef, useMemo, useState } from 'react';
import type { ComponentProps, HTMLAttributes } from 'react';
import { TableVirtuoso } from 'react-virtuoso';

import ClearIcon from '@mui/icons-material/Clear';
import EditIcon from '@mui/icons-material/Edit';
import RssFeedIcon from '@mui/icons-material/RssFeed';
import SearchIcon from '@mui/icons-material/Search';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import Chip from '@mui/material/Chip';
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
import { useTheme } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';
import type { Feed } from '@transcription/common';

import { FeedStatusIndicator } from '../common/FeedStatusIndicator';

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

const GRID_TEMPLATE_COLUMNS = '1.5fr 1fr 1fr 60px';

const sortProperties: Record<string, keyof Feed> = {
  name: 'name',
  type: 'sourceType',
  status: 'status',
};

function VirtuosoTableRow(
  props: ComponentProps<typeof TableRow> & {
    item?: Feed;
    context?: { editingFeedId?: string };
  }
) {
  const { item, context, ...rest } = props;
  const isSelected = !!(item && context?.editingFeedId === item.id);

  return (
    <TableRow
      {...rest}
      component="div"
      hover
      selected={isSelected}
      sx={{
        display: 'grid',
        gridTemplateColumns: GRID_TEMPLATE_COLUMNS,
        width: '100%',
        alignItems: 'center',
        borderBottom: '1px solid',
        borderColor: 'divider',
        borderLeft: '4px solid transparent',
        transition: 'background-color 0.2s ease, border-left-color 0.2s ease',
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

interface FeedConfigurationTableProps {
  feeds: Feed[];
  feedsLoading: boolean;
  editingFeedId: string | undefined;
  onEditFeed: (feed: Feed) => void;
  isSubmitting: boolean;
}

export function FeedConfigurationTable({
  feeds,
  feedsLoading,
  editingFeedId,
  onEditFeed,
  isSubmitting,
}: FeedConfigurationTableProps) {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  // Search filter query for existing feeds column
  const [feedSearchQuery, setFeedSearchQuery] = useState('');

  // Table sorting states (Default sorting is Feed Name ascending)
  const [sortBy, setSortBy] = useState<'name' | 'type' | 'status'>('name');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  const handleRequestSort = (property: 'name' | 'type' | 'status') => {
    const isAsc = sortBy === property && sortDirection === 'asc';
    setSortDirection(isAsc ? 'desc' : 'asc');
    setSortBy(property);
  };

  // Filter list of existing feeds dynamically
  // TODO: add pagination https://linear.app/watchduty/issue/GOO-251/add-pagination-for-feeds-api
  const filteredFeeds = useMemo(() => {
    const query = feedSearchQuery.toLowerCase().trim();
    const result = query
      ? feeds.filter((feed) => {
          const nameMatch = feed.name.toLowerCase().includes(query);
          const tagMatch =
            feed.tags?.some(
              (t) =>
                t.key.toLowerCase().includes(query) ||
                t.value.toLowerCase().includes(query)
            ) ?? false;
          return nameMatch || tagMatch;
        })
      : [...feeds];

    result.sort((a, b) => {
      const prop = sortProperties[sortBy];
      if (!prop) return 0;

      const valA = String(a[prop]).toLowerCase();
      const valB = String(b[prop]).toLowerCase();

      const modifier = sortDirection === 'asc' ? 1 : -1;

      if (valA < valB) return -1 * modifier;
      if (valA > valB) return 1 * modifier;
      return 0;
    });

    return result;
  }, [feeds, feedSearchQuery, sortBy, sortDirection]);

  const tableHeader = (
    <TableRow
      component="div"
      sx={{
        display: 'grid',
        gridTemplateColumns: GRID_TEMPLATE_COLUMNS,
        width: '100%',
        bgcolor: 'background.paper',
      }}
    >
      <TableCell
        component="div"
        sx={{
          fontWeight: 'bold',
          bgcolor: 'background.paper',
        }}
        sortDirection={sortBy === 'name' ? sortDirection : false}
      >
        <TableSortLabel
          active={sortBy === 'name'}
          direction={sortBy === 'name' ? sortDirection : 'asc'}
          onClick={() => handleRequestSort('name')}
        >
          Name
        </TableSortLabel>
      </TableCell>
      <TableCell
        component="div"
        sx={{
          fontWeight: 'bold',
          bgcolor: 'background.paper',
        }}
        sortDirection={sortBy === 'type' ? sortDirection : false}
      >
        <TableSortLabel
          active={sortBy === 'type'}
          direction={sortBy === 'type' ? sortDirection : 'asc'}
          onClick={() => handleRequestSort('type')}
        >
          Type
        </TableSortLabel>
      </TableCell>
      <TableCell
        component="div"
        sx={{
          fontWeight: 'bold',
          bgcolor: 'background.paper',
        }}
        sortDirection={sortBy === 'status' ? sortDirection : false}
      >
        <TableSortLabel
          active={sortBy === 'status'}
          direction={sortBy === 'status' ? sortDirection : 'asc'}
          onClick={() => handleRequestSort('status')}
        >
          Status
        </TableSortLabel>
      </TableCell>

      <TableCell
        component="div"
        align="right"
        sx={{
          fontWeight: 'bold',
          bgcolor: 'background.paper',
        }}
      />
    </TableRow>
  );

  const renderRowContent = (feed: Feed) => {
    const isCurrentlyEditingThis = editingFeedId === feed.id;

    return (
      <>
        {/* Name & ID Metadata */}
        <TableCell
          component="div"
          sx={{
            py: 1,
            display: 'flex',
            flexDirection: 'column',
            borderBottom: 'none',
            minWidth: 0,
          }}
        >
          <Typography variant="body2" sx={{ fontWeight: 600 }}>
            {feed.name}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            <b>Source ID:</b> {feed.sourceFeedId}
          </Typography>
        </TableCell>

        {/* Source Type Chip */}
        <TableCell component="div" sx={{ borderBottom: 'none', minWidth: 0 }}>
          <Chip label={feed.sourceType} size="small" variant="outlined" />
        </TableCell>

        {/* Status Indicator */}
        <TableCell component="div" sx={{ borderBottom: 'none', minWidth: 0 }}>
          <FeedStatusIndicator
            status={feed.status}
            substatus={feed.substatus}
            statusReason={feed.statusReason}
            statusReasonDetail={feed.statusReasonDetail}
            lastHeartbeat={feed.lastHeartbeat}
            lastSpeechSegmentTimestamp={feed.lastSpeechSegmentTimestamp}
          />
        </TableCell>

        {/* Actions Buttons */}
        <TableCell align="right" component="div" sx={{ borderBottom: 'none' }}>
          <IconButton
            size="small"
            onClick={() => onEditFeed(feed)}
            disabled={isSubmitting || isCurrentlyEditingThis}
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
        </TableCell>

        {feed.tags && feed.tags.length > 0 && (
          <TableCell
            component="div"
            sx={{
              gridColumn: '1 / -1',
              borderBottom: 'none',
              pt: 0,
              display: 'flex',
              flexWrap: 'wrap',
              gap: 0.75,
            }}
          >
            {feed.tags.map((tag, i) => (
              <Chip
                key={i}
                label={
                  <Box>
                    <b>{tag.key}</b>: {tag.value}
                  </Box>
                }
                size="small"
              />
            ))}
          </TableCell>
        )}
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
      }}
    >
      <Box
        sx={{
          p: 2,
          pb: 0,
          display: 'flex',
          flexDirection: 'column',
          gap: 2,
        }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <RssFeedIcon color="primary" fontSize="small" />
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Registered feeds
            </Typography>
          </Box>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ fontWeight: 500 }}
          >
            {feeds.length} Feeds
          </Typography>
        </Box>

        <TextField
          fullWidth
          size="small"
          placeholder="Filter feeds by name, tags, or ID..."
          value={feedSearchQuery}
          onChange={(e) => setFeedSearchQuery(e.target.value)}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" color="action" />
                </InputAdornment>
              ),
              endAdornment: feedSearchQuery ? (
                <InputAdornment position="end">
                  <IconButton
                    size="small"
                    onClick={() => setFeedSearchQuery('')}
                  >
                    <ClearIcon fontSize="small" />
                  </IconButton>
                </InputAdornment>
              ) : null,
            },
          }}
          sx={{ mb: 2 }}
        />
      </Box>

      <Divider />

      {feedsLoading ? (
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
              Loading registered feeds...
            </Typography>
          </Stack>
        </Box>
      ) : filteredFeeds.length === 0 ? (
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
            {feedSearchQuery
              ? 'No feeds matching filter query found.'
              : 'No feed found.'}
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
            {feedSearchQuery
              ? 'Refine spelling or delete terms to widen search scope.'
              : 'Register feeds on the left to start listening.'}
          </Typography>
        </Box>
      ) : isMobile ? (
        <TableContainer
          component="div"
          sx={{ flexGrow: 1, overflowY: 'visible' }}
        >
          <Table component="div" sx={{ display: 'block', width: '100%' }}>
            <TableHead component="div" sx={{ display: 'block' }}>
              {tableHeader}
            </TableHead>
            <TableBody component="div" sx={{ display: 'block' }}>
              {filteredFeeds.map((feed) => (
                <VirtuosoTableRow
                  key={feed.id}
                  item={feed}
                  context={{ editingFeedId }}
                >
                  {renderRowContent(feed)}
                </VirtuosoTableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      ) : (
        <TableVirtuoso
          data={filteredFeeds}
          context={{ editingFeedId }}
          computeItemKey={(_index, feed) => feed.id}
          components={VIRTUOSO_COMPONENTS}
          style={{ flexGrow: 1, minHeight: 0 }}
          fixedHeaderContent={() => tableHeader}
          itemContent={(_index, feed) => renderRowContent(feed)}
        />
      )}
    </Card>
  );
}

export default FeedConfigurationTable;
