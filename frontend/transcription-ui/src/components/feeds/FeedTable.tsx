import React, { useMemo, useState } from 'react';
import { Link as RouterLink } from 'react-router';
import { TableVirtuoso } from 'react-virtuoso';

import InventoryIcon from '@mui/icons-material/Inventory';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import SearchIcon from '@mui/icons-material/Search';
import TuneIcon from '@mui/icons-material/Tune';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import Link from '@mui/material/Link';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Paper from '@mui/material/Paper';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import TableSortLabel from '@mui/material/TableSortLabel';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type { Feed } from '@transcription/common';

import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { MultiSelectFilter } from '../common/MultiSelectFilter';

interface FeedTableProps {
  feeds: Feed[];
  isLoading: boolean;
}

interface SortConfig {
  column: 'name' | 'status';
  direction: 'asc' | 'desc';
}

const VirtuosoScroller = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>((props, ref) => <TableContainer {...props} ref={ref} />);
VirtuosoScroller.displayName = 'VirtuosoScroller';

const VirtuosoTableHead = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>((props, ref) => <TableHead {...props} ref={ref} />);
VirtuosoTableHead.displayName = 'VirtuosoTableHead';

const VirtuosoTableBody = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>((props, ref) => <TableBody {...props} ref={ref} />);
VirtuosoTableBody.displayName = 'VirtuosoTableBody';

const VirtuosoTable = React.forwardRef<
  HTMLTableElement,
  React.ComponentProps<typeof Table>
>((props, ref) => (
  <Table
    {...props}
    ref={ref}
    sx={{ borderCollapse: 'separate', tableLayout: 'fixed' }}
  />
));
VirtuosoTable.displayName = 'VirtuosoTable';

function VirtuosoTableRow(props: React.ComponentProps<typeof TableRow>) {
  return <TableRow {...props} hover />;
}

const VIRTUOSO_COMPONENTS = {
  Scroller: VirtuosoScroller,
  Table: VirtuosoTable,
  TableHead: VirtuosoTableHead,
  TableRow: VirtuosoTableRow,
  TableBody: VirtuosoTableBody,
};

function FeedFilterChip({ tag }: { tag: { key: string; value: string } }) {
  return (
    <Chip
      label={
        <Typography variant="body2">
          <b>{tag.key}</b>: {tag.value}
        </Typography>
      }
      size="small"
      variant="filled"
    />
  );
}

function ActionsMenu({ feed }: { feed: Feed }) {
  const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null);

  const handleOpen = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const menuOpen = Boolean(anchorEl);
  const hasSourceUrl = !!feed.sourceUrl;
  const hasArchiveUrl = !!feed.archiveUrl;

  return (
    <>
      <IconButton size="small" onClick={handleOpen} aria-label="feed actions">
        <MoreVertIcon fontSize="small" />
      </IconButton>

      <Menu
        anchorEl={anchorEl}
        open={menuOpen}
        onClose={handleClose}
        slotProps={{
          paper: {
            sx: { minWidth: 180 },
          },
        }}
      >
        <MenuItem
          component={hasSourceUrl ? 'a' : 'li'}
          href={hasSourceUrl ? feed?.sourceUrl : undefined}
          target={hasSourceUrl ? '_blank' : undefined}
          rel="noopener noreferrer"
          disabled={!hasSourceUrl}
          onClick={handleClose}
        >
          <ListItemIcon>
            <OpenInNewOutlinedIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Source URL</ListItemText>
        </MenuItem>
        <MenuItem
          component={hasArchiveUrl ? 'a' : 'li'}
          href={hasArchiveUrl ? feed?.archiveUrl : undefined}
          target={hasArchiveUrl ? '_blank' : undefined}
          rel="noopener noreferrer"
          disabled={!hasArchiveUrl}
          onClick={handleClose}
        >
          <ListItemIcon>
            <InventoryIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Archive URL</ListItemText>
        </MenuItem>
      </Menu>
    </>
  );
}

export function FeedTable({ feeds, isLoading }: FeedTableProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    column: 'name',
    direction: 'asc',
  });

  const [appliedTags, setAppliedTags] = useState<
    { key: string; value: string }[]
  >([]);

  const [appliedStatuses, setAppliedStatuses] = useState<string[]>([]);

  const [appliedSourceTypes, setAppliedSourceTypes] = useState<string[]>([]);

  const tags = useMemo<{ key: string; value: string }[]>(() => {
    const seen = new Set<string>();
    const uniqueTags: { key: string; value: string }[] = [];
    feeds.forEach((feed) => {
      feed.tags?.forEach((tag) => {
        const identifier = `${tag.key}:${tag.value}`;
        if (!seen.has(identifier)) {
          seen.add(identifier);
          uniqueTags.push({ key: tag.key, value: tag.value });
        }
      });
    });
    return uniqueTags;
  }, [feeds]);

  const sourceTypes = useMemo<string[]>(() => {
    const seen = new Set<string>();
    feeds.forEach((feed) => {
      if (feed.sourceType) {
        seen.add(feed.sourceType);
      }
    });
    return Array.from(seen).sort();
  }, [feeds]);

  const handleRequestSort = (property: 'name' | 'status') => {
    setSortConfig((prev) => ({
      column: property,
      direction:
        prev.column === property && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  const filteredAndSortedFeeds = useMemo(() => {
    const query = searchQuery.toLowerCase().trim();
    let filtered = feeds;

    if (query) {
      filtered = filtered.filter((feed) => {
        const nameMatches = feed.name.toLowerCase().includes(query);
        const tagMatches =
          feed.tags?.some(
            (tag) =>
              tag.key.toLowerCase().includes(query) ||
              tag.value.toLowerCase().includes(query)
          ) ?? false;
        return nameMatches || tagMatches;
      });
    }

    if (appliedTags.length > 0) {
      filtered = filtered.filter((feed) => {
        return appliedTags.every((appliedTag) =>
          feed.tags?.some(
            (tag) =>
              tag.key === appliedTag.key && tag.value === appliedTag.value
          )
        );
      });
    }

    if (appliedStatuses.length > 0) {
      filtered = filtered.filter((feed) => {
        const capitalizedStatus =
          feed.status.charAt(0).toUpperCase() + feed.status.slice(1);
        return appliedStatuses.includes(capitalizedStatus);
      });
    }

    if (appliedSourceTypes.length > 0) {
      filtered = filtered.filter((feed) =>
        appliedSourceTypes.includes(feed.sourceType)
      );
    }

    return filtered.sort((a, b) => {
      let comparison = 0;
      if (sortConfig.column === 'name') {
        comparison = a.name.localeCompare(b.name);
      } else if (sortConfig.column === 'status') {
        comparison = a.status.localeCompare(b.status);
      }
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
  }, [
    feeds,
    searchQuery,
    appliedTags,
    appliedStatuses,
    appliedSourceTypes,
    sortConfig,
  ]);

  return (
    <Paper
      variant="outlined"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
        overflow: 'hidden',
        borderRadius: 2,
        boxShadow: '0 4px 20px rgba(0,0,0,0.05)',
      }}
    >
      <Box
        sx={{
          p: 2,
          borderBottom: 1,
          borderColor: 'divider',
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          bgcolor: 'background.paper',
        }}
      >
        <TextField
          fullWidth
          size="small"
          placeholder="Search feeds..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon color="action" />
                </InputAdornment>
              ),
            },
          }}
          sx={{
            maxWidth: 600,
          }}
        />

        <TuneIcon color="action" sx={{ ml: 1 }} />
        <Box sx={{ width: 250, maxWidth: '100%' }}>
          <MultiSelectFilter
            label="Source Type"
            options={sourceTypes}
            value={appliedSourceTypes}
            onChange={setAppliedSourceTypes}
            size="small"
          />
        </Box>
        <Box sx={{ width: 250, maxWidth: '100%' }}>
          <MultiSelectFilter
            label="Status"
            options={['Active', 'Inactive']}
            value={appliedStatuses}
            onChange={setAppliedStatuses}
            size="small"
          />
        </Box>
        <Box sx={{ width: 250, maxWidth: '100%' }}>
          <MultiSelectFilter
            label="Tags"
            options={tags}
            value={appliedTags}
            onChange={setAppliedTags}
            size="small"
            groupBy={(tag) => tag.key}
            getOptionLabel={(tag) => `${tag.key}: ${tag.value}`}
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

        {filteredAndSortedFeeds.length !== feeds.length && (
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ ml: 'auto' }}
          >
            Showing {filteredAndSortedFeeds.length} of {feeds.length} feeds
          </Typography>
        )}
      </Box>

      {isLoading ? (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            flexGrow: 1,
          }}
        >
          <CircularProgress />
        </Box>
      ) : (
        <TableVirtuoso
          data={filteredAndSortedFeeds}
          computeItemKey={(_index, feed) => feed.id}
          components={VIRTUOSO_COMPONENTS}
          fixedHeaderContent={() => (
            <TableRow>
              <TableCell
                sx={{
                  bgcolor: 'background.paper',
                  fontWeight: 'bold',
                }}
              >
                <TableSortLabel
                  active={sortConfig.column === 'name'}
                  direction={
                    sortConfig.column === 'name' ? sortConfig.direction : 'asc'
                  }
                  onClick={() => handleRequestSort('name')}
                >
                  Name
                </TableSortLabel>
              </TableCell>
              <TableCell
                sx={{
                  bgcolor: 'background.paper',
                  fontWeight: 'bold',
                }}
              >
                <TableSortLabel
                  active={sortConfig.column === 'status'}
                  direction={
                    sortConfig.column === 'status'
                      ? sortConfig.direction
                      : 'asc'
                  }
                  onClick={() => handleRequestSort('status')}
                >
                  Status
                </TableSortLabel>
              </TableCell>
              <TableCell
                sx={{
                  bgcolor: 'background.paper',
                  fontWeight: 'bold',
                }}
              >
                Tags
              </TableCell>
              <TableCell>{/* Empty header cell */}</TableCell>
            </TableRow>
          )}
          itemContent={(_index, feed) => (
            <>
              <TableCell
                sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'flex-start',
                }}
              >
                <Link
                  component={RouterLink}
                  to={`/transcripts?feedId=${feed.id}`}
                  variant="body1"
                  sx={{
                    fontWeight: 500,
                    textOverflow: 'ellipsis',
                    maxWidth: '100%',
                  }}
                  noWrap
                >
                  {feed.name}
                </Link>
                <Typography variant="caption" color="text.secondary">
                  {feed.sourceType}
                </Typography>
              </TableCell>
              <TableCell sx={{ verticalAlign: 'top', width: '100%' }}>
                <FeedStatusIndicator
                  status={feed.status}
                  lastHeartbeat={feed.lastHeartbeat}
                />
              </TableCell>
              <TableCell sx={{ verticalAlign: 'top', width: '100%' }}>
                {feed.tags && feed.tags.length > 0 ? (
                  <Box
                    sx={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: 0.5,
                      alignItems: 'flex-start',
                    }}
                  >
                    {feed.tags.map((tag) => (
                      <FeedFilterChip key={tag.key} tag={tag} />
                    ))}
                  </Box>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    -
                  </Typography>
                )}
              </TableCell>
              <TableCell align="right" sx={{ verticalAlign: 'top' }}>
                <ActionsMenu feed={feed} />
              </TableCell>
            </>
          )}
        />
      )}
    </Paper>
  );
}

export default FeedTable;
