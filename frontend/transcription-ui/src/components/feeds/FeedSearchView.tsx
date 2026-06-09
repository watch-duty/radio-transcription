import { useEffect, useMemo, useState } from 'react';

import {
  Autocomplete,
  Box,
  Chip,
  CircularProgress,
  TextField,
  Typography,
} from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { type Feed } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { type FeedFilters, FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
  condensed?: boolean;
  selectedFeedId?: string | null;
  onFeedSelect?: (feedId: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds
const QUERY_DEBOUNCE_TIME_MS = 300;

interface CondensedFeedSearchResultsProps {
  feeds: Feed[];
  selectedFeed: Feed | null;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
  feedsLoading: boolean;
  onFeedSelect?: (feedId: string) => void;
}

function CondensedFeedSearchResults({
  feeds,
  selectedFeed,
  filters,
  onFiltersChange,
  feedsLoading,
  onFeedSelect,
}: CondensedFeedSearchResultsProps) {
  const [localInputValue, setLocalInputValue] = useState(
    selectedFeed?.name || ''
  );
  const [isFocused, setIsFocused] = useState(false);
  const [prevSelectedFeedId, setPrevSelectedFeedId] = useState<
    string | undefined
  >(selectedFeed?.id);

  // Sync search input state with selectedFeed prop changes.
  // We use React's recommended render-phase state adjustment pattern instead of useEffect
  // to avoid double renders and visual flashes of stale values.
  // We compare feed IDs (primitive string) to prevent background query refetches from
  // changing object references and wiping out active user typing.
  if (selectedFeed?.id !== prevSelectedFeedId) {
    setPrevSelectedFeedId(selectedFeed?.id);
    setLocalInputValue(selectedFeed?.name || '');
  }

  return (
    <Box sx={{ width: '50%', textAlign: 'left' }}>
      <Autocomplete
        disablePortal
        options={feeds}
        // Prevents client-side filtering since all filtering is done server-side.
        filterOptions={(x) => x}
        getOptionLabel={(option) => option.name}
        size="small"
        value={selectedFeed}
        inputValue={localInputValue}
        onChange={(_, option) => {
          if (option && onFeedSelect) {
            onFeedSelect(option.id);
          }
        }}
        onInputChange={(_, newInputValue, reason) => {
          if (reason === 'reset' && isFocused) {
            return;
          }
          setLocalInputValue(newInputValue);
          if (reason === 'input' || reason === 'clear') {
            onFiltersChange({ ...filters, searchQuery: newInputValue });
          }
        }}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setIsFocused(false)}
        loading={feedsLoading}
        loadingText={
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CircularProgress size={16} /> Loading feeds...
          </Box>
        }
        renderInput={(params) => (
          <TextField
            {...params}
            label="Select feed"
            placeholder="Search or select feed..."
            slotProps={{
              ...params.slotProps,
            }}
          />
        )}
        renderOption={(props, option) => {
          const { key, ...optionProps } = props;
          return (
            <Box
              key={key}
              component="li"
              {...optionProps}
              sx={{
                display: 'block !important',
                textAlign: 'left !important',
                width: '100%',
                borderBottom: '1px solid',
                borderColor: 'divider',
                py: 1,
                px: 2,
                '&:last-child': {
                  borderBottom: 'none',
                },
              }}
            >
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                }}
              >
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  {option.name}
                </Typography>
                <FeedStatusIndicator
                  status={option.status}
                  substatus={option.substatus}
                  lastHeartbeat={option.lastHeartbeat}
                />
              </Box>
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  gap: 0.75,
                  mt: 0.5,
                }}
              >
                <Chip
                  label={option.sourceType}
                  size="small"
                  variant="outlined"
                />
                {option.tags && option.tags.length > 0 && (
                  <>
                    {option.tags.map((tag, i) => (
                      <Chip
                        key={`feed-${option.id}-tag-${i}`}
                        label={
                          <Typography variant="body2">
                            <b>{tag.key}</b>: {tag.value}
                          </Typography>
                        }
                        size="small"
                        variant="filled"
                      />
                    ))}
                  </>
                )}
              </Box>
            </Box>
          );
        }}
      />
    </Box>
  );
}

interface TableFeedSearchResultsProps {
  title: string;
  feeds: Feed[];
  tags: { key: string; value: string }[];
  feedsLoading: boolean;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
}

function TableFeedSearchResults({
  title,
  feeds,
  tags,
  feedsLoading,
  filters,
  onFiltersChange,
}: TableFeedSearchResultsProps) {
  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        height: 'calc(100vh - 100px)',
      }}
    >
      <FeedTable
        title={title}
        feeds={feeds}
        tags={tags}
        isLoading={feedsLoading}
        filters={filters}
        onFiltersChange={onFiltersChange}
      />
    </Box>
  );
}

export function FeedSearchView({
  title,
  onError,
  condensed = false,
  selectedFeedId = null,
  onFeedSelect,
}: FeedSearchViewProps) {
  const { token } = useAuth();

  const [filters, setFilters] = useState<FeedFilters>({
    searchQuery: '',
    sourceTypes: [],
    statuses: [],
    tags: [],
  });

  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState(
    filters.searchQuery
  );

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchQuery(filters.searchQuery);
    }, QUERY_DEBOUNCE_TIME_MS);
    return () => clearTimeout(handler);
  }, [filters.searchQuery]);

  const {
    data: feeds,
    error: feedsError,
    isLoading: feedsLoading,
  } = useQuery({
    queryKey: [
      'listFeeds',
      token,
      debouncedSearchQuery,
      filters.sourceTypes,
      filters.sourceTypes.length,
      filters.statuses,
      filters.statuses.length,
      filters.tags,
      filters.tags.length,
    ],
    queryFn: () =>
      listFeeds(token!, {
        name: debouncedSearchQuery || undefined,
        sourceTypes:
          filters.sourceTypes.length > 0 ? filters.sourceTypes : undefined,
        statuses: filters.statuses.length > 0 ? filters.statuses : undefined,
        tags: filters.tags.length > 0 ? filters.tags : undefined,
      }),
    enabled: !!token,
    refetchOnWindowFocus: false,
    refetchInterval: FEED_REFETCH_INTERVAL_MS,
  });

  const { data: allFeeds = [] } = useQuery({
    queryKey: ['listFeeds', token, '', [], 0, [], 0, [], 0],
    queryFn: () => listFeeds(token!, {}),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  const selectedFeed: Feed | null = useMemo(() => {
    return allFeeds.find((f) => f.id === selectedFeedId) || null;
  }, [allFeeds, selectedFeedId]);

  const tags = useMemo<{ key: string; value: string }[]>(() => {
    const seen = new Set<string>();
    const uniqueTags: { key: string; value: string }[] = [];
    const sourceFeeds = allFeeds || feeds || [];
    sourceFeeds.forEach((feed) => {
      feed.tags?.forEach((tag) => {
        const identifier = `${tag.key}:${tag.value}`;
        if (!seen.has(identifier)) {
          seen.add(identifier);
          uniqueTags.push({ key: tag.key, value: tag.value });
        }
      });
    });
    return uniqueTags.sort(
      (a, b) => a.key.localeCompare(b.key) || a.value.localeCompare(b.value)
    );
  }, [feeds, allFeeds]);

  const sortedFeedsForAutocomplete = useMemo(() => {
    return [...(feeds ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

  if (condensed) {
    return (
      <CondensedFeedSearchResults
        feeds={sortedFeedsForAutocomplete}
        selectedFeed={selectedFeed}
        filters={filters}
        onFiltersChange={setFilters}
        feedsLoading={feedsLoading}
        onFeedSelect={onFeedSelect}
      />
    );
  }

  return (
    <TableFeedSearchResults
      title={title}
      feeds={feeds ?? []}
      tags={tags}
      feedsLoading={feedsLoading}
      filters={filters}
      onFiltersChange={setFilters}
    />
  );
}

export default FeedSearchView;
