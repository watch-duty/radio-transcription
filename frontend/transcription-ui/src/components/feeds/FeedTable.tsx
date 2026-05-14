import React, { useMemo, useState } from 'react';
import { TableVirtuoso } from 'react-virtuoso';
import { Link as RouterLink } from 'react-router';

import InventoryIcon from '@mui/icons-material/Inventory';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import SearchIcon from '@mui/icons-material/Search';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
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
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';
import type { Feed } from '@transcription/common';

import { FeedStatusIndicator } from '../common/FeedStatusIndicator';

interface FeedTableProps {
  feeds: Feed[];
  isLoading: boolean;
}

export function FeedTable({ feeds, isLoading }: FeedTableProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [orderBy, setOrderBy] = useState<'name' | 'status'>('name');
  const [orderDirection, setOrderDirection] = useState<'asc' | 'desc'>('asc');

  // State for the high-performance shared menu using absolute screen pixel coordinates.
  // This entirely bypasses DOM measurement bugs caused by virtualized scrollers.
  const [menuAnchorPosition, setMenuAnchorPosition] = useState<{
    top: number;
    left: number;
  } | null>(null);
  const [activeFeed, setActiveFeed] = useState<Feed | null>(null);

  const handleRequestSort = (property: 'name' | 'status') => {
    const isAsc = orderBy === property && orderDirection === 'asc';
    setOrderDirection(isAsc ? 'desc' : 'asc');
    setOrderBy(property);
  };

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>, feed: Feed) => {
    event.stopPropagation();
    const rect = event.currentTarget.getBoundingClientRect();
    setMenuAnchorPosition({
      top: rect.bottom,
      left: rect.right,
    });
    setActiveFeed(feed);
  };

  const handleMenuClose = () => {
    setMenuAnchorPosition(null);
    setActiveFeed(null);
  };

  const filteredAndSortedFeeds = useMemo(() => {
    const query = searchQuery.toLowerCase().trim();
    const filtered = feeds.filter((feed) => {
      if (!query) return true;
      const nameMatches = feed.name.toLowerCase().includes(query);
      const tagMatches =
        feed.tags?.some(
          (tag) =>
            tag.key.toLowerCase().includes(query) ||
            tag.value.toLowerCase().includes(query)
        ) ?? false;
      return nameMatches || tagMatches;
    });

    return filtered.sort((a, b) => {
      let comparison = 0;
      if (orderBy === 'name') {
        comparison = a.name.localeCompare(b.name);
      } else if (orderBy === 'status') {
        comparison = a.status.localeCompare(b.status);
      }
      return orderDirection === 'asc' ? comparison : -comparison;
    });
  }, [feeds, searchQuery, orderBy, orderDirection]);

  const hasSourceUrl = !!activeFeed?.sourceUrl;
  const hasArchiveUrl = !!activeFeed?.archiveUrl;

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
        {filteredAndSortedFeeds.length !== feeds.length && <Typography variant="body2" color="text.secondary" sx={{ ml: 'auto' }}>
          Showing {filteredAndSortedFeeds.length} of {feeds.length} feeds
        </Typography>}
      </Box>

      {isLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flexGrow: 1 }}>
          <CircularProgress />
        </Box>
      ) : (
        <TableVirtuoso
          data={filteredAndSortedFeeds}
          computeItemKey={(_index, feed) => feed.id}
          components={{
            Scroller: React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>((props, ref) => (
              <TableContainer {...props} ref={ref} />
            )),
            Table: (props) => (
              <Table {...props} sx={{ borderCollapse: 'separate', tableLayout: 'fixed' }} />
            ),
            TableHead: React.forwardRef<HTMLTableSectionElement, React.HTMLAttributes<HTMLTableSectionElement>>((props, ref) => (
              <TableHead {...props} ref={ref} />
            )),
            TableRow: (props) => (
              <TableRow {...props} hover />
            ),
            TableBody: React.forwardRef<HTMLTableSectionElement, React.HTMLAttributes<HTMLTableSectionElement>>((props, ref) => (
              <TableBody {...props} ref={ref} />
            )),
          }}
          fixedHeaderContent={() => (
            <TableRow>
              <TableCell sx={{ width: '35%', bgcolor: 'background.paper', fontWeight: 'bold' }}>
                <TableSortLabel
                  active={orderBy === 'name'}
                  direction={orderBy === 'name' ? orderDirection : 'asc'}
                  onClick={() => handleRequestSort('name')}
                >
                  Name
                </TableSortLabel>
              </TableCell>
              <TableCell sx={{ width: '25%', bgcolor: 'background.paper', fontWeight: 'bold' }}>
                <TableSortLabel
                  active={orderBy === 'status'}
                  direction={orderBy === 'status' ? orderDirection : 'asc'}
                  onClick={() => handleRequestSort('status')}
                >
                  Status
                </TableSortLabel>
              </TableCell>
              <TableCell sx={{ width: '30%', bgcolor: 'background.paper', fontWeight: 'bold' }}>
                Tags
              </TableCell>
              <TableCell>{/* Empty header cell */}</TableCell>
            </TableRow>
          )}
          itemContent={(_index, feed) => (
            <>
              <TableCell sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}>
                <Link component={RouterLink} to={`/transcripts?feedId=${feed.id}`} variant="body1" sx={{ fontWeight: 500 }}>
                  {feed.name}
                </Link>
                <Typography variant="caption" color="text.secondary">
                  {feed.sourceType}
                </Typography>
              </TableCell>
              <TableCell sx={{ verticalAlign: 'top' }}>
                <FeedStatusIndicator status={feed.status} lastHeartbeat={feed.lastHeartbeat} />
              </TableCell>
              <TableCell sx={{ verticalAlign: 'top' }}>
                {feed.tags && feed.tags.length > 0 ? (
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    {feed.tags.map((tag, i) => (
                      <Typography key={i} variant="body2" sx={{ fontFamily: 'monospace' }}>
                        <b>{tag.key}</b>: {tag.value}
                      </Typography>
                    ))}
                  </Box>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    None
                  </Typography>
                )}
              </TableCell>
              <TableCell align="right" sx={{ verticalAlign: 'top' }}>
                <IconButton
                  size="small"
                  onClick={(e) => handleMenuOpen(e, feed)}
                  aria-label="feed actions"
                >
                  <MoreVertIcon fontSize="small" />
                </IconButton>
              </TableCell>
            </>
          )}
        />
      )}

      <Menu
        anchorReference="anchorPosition"
        anchorPosition={
          menuAnchorPosition
            ? { top: menuAnchorPosition.top, left: menuAnchorPosition.left }
            : undefined
        }
        open={Boolean(menuAnchorPosition)}
        onClose={handleMenuClose}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
        slotProps={{
          paper: {
            sx: { minWidth: 180 },
          },
        }}
      >
        <MenuItem
          component={hasSourceUrl ? 'a' : 'li'}
          href={hasSourceUrl ? activeFeed?.sourceUrl : undefined}
          target={hasSourceUrl ? '_blank' : undefined}
          rel="noopener noreferrer"
          disabled={!hasSourceUrl}
          onClick={handleMenuClose}
        >
          <ListItemIcon>
            <OpenInNewOutlinedIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Source URL</ListItemText>
        </MenuItem>
        <MenuItem
          component={hasArchiveUrl ? 'a' : 'li'}
          href={hasArchiveUrl ? activeFeed?.archiveUrl : undefined}
          target={hasArchiveUrl ? '_blank' : undefined}
          rel="noopener noreferrer"
          disabled={!hasArchiveUrl}
          onClick={handleMenuClose}
        >
          <ListItemIcon>
            <InventoryIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText>Archive URL</ListItemText>
        </MenuItem>
      </Menu>
    </Paper>
  );
}

export default FeedTable;
