import React, { useMemo, useState } from 'react';
import { Link as RouterLink } from 'react-router';
import { TableVirtuoso } from 'react-virtuoso';

import InventoryIcon from '@mui/icons-material/Inventory';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import SearchIcon from '@mui/icons-material/Search';
import Box from '@mui/material/Box';
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

interface FeedTableProps {
  feeds: Feed[];
  isLoading: boolean;
}

interface SortConfig {
  column: 'name' | 'status';
  direction: 'asc' | 'desc';
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

  const handleRequestSort = (property: 'name' | 'status') => {
    setSortConfig((prev) => ({
      column: property,
      direction:
        prev.column === property && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
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
      if (sortConfig.column === 'name') {
        comparison = a.name.localeCompare(b.name);
      } else if (sortConfig.column === 'status') {
        comparison = a.status.localeCompare(b.status);
      }
      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
  }, [feeds, searchQuery, sortConfig]);

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
          components={{
            Scroller: React.forwardRef<
              HTMLDivElement,
              React.HTMLAttributes<HTMLDivElement>
            >((props, ref) => <TableContainer {...props} ref={ref} />),
            Table: (props) => (
              <Table
                {...props}
                sx={{ borderCollapse: 'separate', tableLayout: 'fixed' }}
              />
            ),
            TableHead: React.forwardRef<
              HTMLTableSectionElement,
              React.HTMLAttributes<HTMLTableSectionElement>
            >((props, ref) => <TableHead {...props} ref={ref} />),
            TableRow: (props) => <TableRow {...props} hover />,
            TableBody: React.forwardRef<
              HTMLTableSectionElement,
              React.HTMLAttributes<HTMLTableSectionElement>
            >((props, ref) => <TableBody {...props} ref={ref} />),
          }}
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
                    width: '100%',
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
                    sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}
                  >
                    {feed.tags.map((tag, i) => (
                      <Typography
                        key={i}
                        variant="body2"
                        sx={{ fontFamily: 'monospace' }}
                      >
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
