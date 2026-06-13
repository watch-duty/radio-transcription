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
import { toSourceTypeString } from '../../utils/textUtils';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { type FeedFilters, FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
  condensed?: boolean;
  onFeedSelect?: (feedId: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds
const QUERY_DEBOUNCE_TIME_MS = 300;

interface CondensedFeedSearchResultsProps {
  feeds: Feed[];
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
  feedsLoading: boolean;
  feedTotal: number;
  onFeedSelect?: (feedId: string) => void;
}

function CondensedFeedSearchResults({
  feeds,
  filters,
  onFiltersChange,
  feedsLoading,
  onFeedSelect,
}: CondensedFeedSearchResultsProps) {
  const [inputValue, setInputValue] = useState('');

  return (
    <Box sx={{ width: '50%', textAlign: 'left' }}>
      <Autocomplete
        disablePortal
        options={feeds}
        // Prevents client-side filtering since all filtering is done server-side.
        filterOptions={(x) => x}
        getOptionLabel={(option) => option.name}
        size="small"
        value={null}
        inputValue={inputValue}
        onChange={(_, option) => {
          if (option && onFeedSelect) {
            onFeedSelect(option.id);
            setInputValue('');
            onFiltersChange({ ...filters, searchQuery: '' });
          }
        }}
        onInputChange={(_, newInputValue) => {
          setInputValue(newInputValue);
          onFiltersChange({ ...filters, searchQuery: newInputValue });
        }}
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
                  statusReason={option.statusReason}
                  quarantineReason={option.quarantineReason}
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
                  label={toSourceTypeString(option.sourceType)}
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
  feedTotal: number;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
}

function TableFeedSearchResults({
  title,
  feeds,
  tags,
  feedsLoading,
  feedTotal,
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
        feedTotal={feedTotal}
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
    data: feedsData,
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

  const feeds = useMemo(() => feedsData?.feeds ?? [], [feedsData]);
  const feedTotal = feedsData?.total ?? 0;

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  // TODO: https://linear.app/watchduty/issue/GOO-575 - Remove allFeeds once the tags are computed in the backend
  const { data: allFeedData = { feeds: [], total: 0 } } = useQuery({
    queryKey: ['listFeeds', token, '', [], 0, [], 0, [], 0],
    queryFn: () => listFeeds(token!, {}),
    enabled: !!token && !condensed,
    refetchOnWindowFocus: false,
  });

  const allFeeds = useMemo(() => allFeedData?.feeds ?? [], [allFeedData]);

  // TODO: https://linear.app/watchduty/issue/GOO-575 - Provide filter tags in backend
  const tags = useMemo<{ key: string; value: string }[]>(() => {
    const seen = new Set<string>();
    const uniqueTags: { key: string; value: string }[] = [];
    const sourceFeeds = allFeeds || [];
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
  }, [allFeeds]);

  const sortedFeedsForAutocomplete = useMemo(() => {
    return [...(feeds ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

  if (condensed) {
    return (
      <CondensedFeedSearchResults
        feeds={sortedFeedsForAutocomplete}
        filters={filters}
        onFiltersChange={setFilters}
        feedsLoading={feedsLoading}
        feedTotal={feedTotal}
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
      feedTotal={feedTotal}
      filters={filters}
      onFiltersChange={setFilters}
    />
  );
}

export default FeedSearchView;
