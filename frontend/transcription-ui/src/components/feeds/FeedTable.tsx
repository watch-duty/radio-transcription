import React, { useMemo, useState } from 'react';
import { Link as RouterLink } from 'react-router';
import { TableVirtuoso } from 'react-virtuoso';

import CheckBoxIcon from '@mui/icons-material/CheckBox';
import CheckBoxOutlineBlankIcon from '@mui/icons-material/CheckBoxOutlineBlank';
import InventoryIcon from '@mui/icons-material/Inventory';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import OpenInNewOutlinedIcon from '@mui/icons-material/OpenInNewOutlined';
import SearchIcon from '@mui/icons-material/Search';
import TuneIcon from '@mui/icons-material/Tune';
import FilterIcon from '@mui/icons-material/Tune';
import Autocomplete from '@mui/material/Autocomplete';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
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
import Popover from '@mui/material/Popover';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import TableSortLabel from '@mui/material/TableSortLabel';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { Feed } from '@transcription/common';

import { FeedStatusIndicator } from '../common/FeedStatusIndicator';

interface FeedStatusFilterProps {
  selectedStatuses: string[];
  onChange: (selectedStatuses: string[]) => void;
  size?: 'small' | 'medium';
}

function FeedStatusFilter({
  selectedStatuses,
  onChange,
  size,
}: FeedStatusFilterProps) {
  const options = ['Active', 'Inactive'];

  return (
    <Autocomplete
      multiple={true}
      options={options}
      value={selectedStatuses}
      onChange={(_, value) => onChange(value)}
      disableCloseOnSelect={true}
      getOptionLabel={(option) => option}
      size={size}
      renderOption={(props, option, { selected }) => {
        const { key, ...optionProps } = props;
        const SelectionIcon = selected
          ? CheckBoxIcon
          : CheckBoxOutlineBlankIcon;

        return (
          <li key={key} {...optionProps}>
            <SelectionIcon
              fontSize="small"
              style={{ marginRight: 8, padding: 9, boxSizing: 'content-box' }}
            />
            {option}
          </li>
        );
      }}
      renderInput={(params) => (
        <TextField {...params} label="Status" placeholder="" size={size} />
      )}
      renderValue={(value) => (
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
          {value.map((status) => (
            <Chip key={status} label={status} size="small" variant="filled" />
          ))}
        </Box>
      )}
    />
  );
}

interface FeedSourceTypeFilterProps {
  sourceTypes: string[];
  selectedSourceTypes: string[];
  onChange: (selectedSourceTypes: string[]) => void;
  size?: 'small' | 'medium';
}

function FeedSourceTypeFilter({
  sourceTypes,
  selectedSourceTypes,
  onChange,
  size,
}: FeedSourceTypeFilterProps) {
  return (
    <Autocomplete
      multiple={true}
      options={sourceTypes}
      value={selectedSourceTypes}
      onChange={(_, value) => onChange(value)}
      disableCloseOnSelect={true}
      getOptionLabel={(option) => option}
      size={size}
      renderOption={(props, option, { selected }) => {
        const { key, ...optionProps } = props;
        const SelectionIcon = selected
          ? CheckBoxIcon
          : CheckBoxOutlineBlankIcon;

        return (
          <li key={key} {...optionProps}>
            <SelectionIcon
              fontSize="small"
              style={{ marginRight: 8, padding: 9, boxSizing: 'content-box' }}
            />
            {option}
          </li>
        );
      }}
      renderInput={(params) => (
        <TextField {...params} label="Source Type" placeholder="" size={size} />
      )}
      renderValue={(value) => (
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
          {value.map((sourceType) => (
            <Chip
              key={sourceType}
              label={sourceType}
              size="small"
              variant="filled"
            />
          ))}
        </Box>
      )}
    />
  );
}

interface FeedTableProps {
  feeds: Feed[];
  isLoading: boolean;
  collapse?: boolean;
}

interface SortConfig {
  column: 'name' | 'status';
  direction: 'asc' | 'desc';
}

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

interface FeedFilterProps {
  tags: { key: string; value: string }[];
  selectedTags: { key: string; value: string }[];
  onChange: (selectedTags: { key: string; value: string }[]) => void;
  size?: 'small' | 'medium';
}

function FeedFilter({ tags, selectedTags, onChange, size }: FeedFilterProps) {
  return (
    <Autocomplete
      multiple={true}
      options={tags}
      groupBy={(tag) => tag.key}
      value={selectedTags}
      onChange={(_, value) => onChange(value)}
      isOptionEqualToValue={(option, value) =>
        option.key === value.key && option.value === value.value
      }
      disableCloseOnSelect={true}
      getOptionLabel={(option) => `${option.key}: ${option.value}`}
      size={size}
      renderOption={(props, option, { selected }) => {
        const { key, ...optionProps } = props;
        const SelectionIcon = selected
          ? CheckBoxIcon
          : CheckBoxOutlineBlankIcon;

        return (
          <li key={key} {...optionProps}>
            <SelectionIcon
              fontSize="small"
              style={{ marginRight: 8, padding: 9, boxSizing: 'content-box' }}
            />
            {option.value}
          </li>
        );
      }}
      renderGroup={(group) => (
        <li key={group.key}>
          <Box sx={{ padding: 1, fontWeight: 'bold' }}>{group.group}</Box>
          <ul style={{ padding: 0 }}>{group.children}</ul>
        </li>
      )}
      renderInput={(params) => (
        <TextField {...params} label="Tags" placeholder="" size={size} />
      )}
      renderValue={(value) => (
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
          {value.map((tag) => (
            <FeedFilterChip key={tag.key} tag={tag} />
          ))}
        </Box>
      )}
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

export function FeedTable({
  feeds,
  isLoading,
  collapse = true,
}: FeedTableProps) {
  const [filterAnchorEl, setFilterAnchorEl] =
    React.useState<HTMLElement | null>(null);

  const [searchQuery, setSearchQuery] = useState('');
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    column: 'name',
    direction: 'asc',
  });

  const [selectedTags, setSelectedTags] = useState<
    { key: string; value: string }[]
  >([]);
  const [appliedTags, setAppliedTags] = useState<
    { key: string; value: string }[]
  >([]);

  const [selectedStatuses, setSelectedStatuses] = useState<string[]>([]);
  const [appliedStatuses, setAppliedStatuses] = useState<string[]>([]);

  const [selectedSourceTypes, setSelectedSourceTypes] = useState<string[]>([]);
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

  const handleFilterOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterAnchorEl(event.currentTarget);
  };

  const handleFilterClose = () => {
    setSelectedTags(appliedTags);
    setSelectedStatuses(appliedStatuses);
    setSelectedSourceTypes(appliedSourceTypes);
    setFilterAnchorEl(null);
  };

  const handleFilterApply = () => {
    setAppliedTags(selectedTags);
    setAppliedStatuses(selectedStatuses);
    setAppliedSourceTypes(selectedSourceTypes);
    setFilterAnchorEl(null);
  };

  const handleFilterClear = () => {
    setSelectedTags([]);
    setAppliedTags([]);
    setSelectedStatuses([]);
    setAppliedStatuses([]);
    setSelectedSourceTypes([]);
    setAppliedSourceTypes([]);
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

        {collapse ? (
          <>
            <Tooltip title="Filter feeds">
              <Badge
                color="primary"
                badgeContent={
                  appliedTags.length +
                  appliedStatuses.length +
                  appliedSourceTypes.length
                }
                invisible={
                  appliedTags.length +
                    appliedStatuses.length +
                    appliedSourceTypes.length ===
                  0
                }
              >
                <Button
                  color="primary"
                  variant="outlined"
                  sx={{
                    minWidth: 0,
                    p: 0.75,
                    textTransform: 'none',
                    display: 'flex',
                    gap: 1,
                  }}
                  aria-label="filter"
                  onClick={handleFilterOpen}
                >
                  <FilterIcon />
                  Filters
                </Button>
              </Badge>
            </Tooltip>
            <Popover
              open={Boolean(filterAnchorEl)}
              anchorEl={filterAnchorEl}
              onClose={handleFilterClose}
              transitionDuration={0}
              anchorOrigin={{
                vertical: 'bottom',
                horizontal: 'left',
              }}
              transformOrigin={{
                vertical: 'top',
                horizontal: 'left',
              }}
              sx={{ zIndex: 1300 }}
            >
              <Box
                sx={{
                  p: 2,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 2,
                  width: 320,
                  maxWidth: '100%',
                }}
              >
                <FeedSourceTypeFilter
                  sourceTypes={sourceTypes}
                  selectedSourceTypes={selectedSourceTypes}
                  onChange={setSelectedSourceTypes}
                />
                <FeedStatusFilter
                  selectedStatuses={selectedStatuses}
                  onChange={setSelectedStatuses}
                />
                <FeedFilter
                  tags={tags}
                  selectedTags={selectedTags}
                  onChange={setSelectedTags}
                />
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                  }}
                >
                  <Button size="small" onClick={handleFilterClear}>
                    Clear
                  </Button>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button size="small" onClick={handleFilterClose}>
                      Cancel
                    </Button>
                    <Button
                      size="small"
                      variant="contained"
                      color="primary"
                      onClick={handleFilterApply}
                    >
                      Apply
                    </Button>
                  </Box>
                </Box>
              </Box>
            </Popover>
          </>
        ) : (
          <>
            <TuneIcon color="action" sx={{ ml: 1 }} />
            <Box sx={{ width: 250, maxWidth: '100%' }}>
              <FeedSourceTypeFilter
                sourceTypes={sourceTypes}
                selectedSourceTypes={appliedSourceTypes}
                onChange={setAppliedSourceTypes}
                size="small"
              />
            </Box>
            <Box sx={{ width: 250, maxWidth: '100%' }}>
              <FeedStatusFilter
                selectedStatuses={appliedStatuses}
                onChange={setAppliedStatuses}
                size="small"
              />
            </Box>
            <Box sx={{ width: 250, maxWidth: '100%' }}>
              <FeedFilter
                tags={tags}
                selectedTags={appliedTags}
                onChange={setAppliedTags}
                size="small"
              />
            </Box>
          </>
        )}

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
