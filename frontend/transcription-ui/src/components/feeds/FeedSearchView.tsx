import { useEffect, useMemo, useState } from 'react';

import TuneIcon from '@mui/icons-material/Tune';
import { Autocomplete, Box, Chip, TextField, Typography } from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import { type Feed, SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { MultiSelectFilter } from '../common/MultiSelectFilter';
import { type FeedFilters, FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
  collapsed?: boolean;
  selectedFeedId?: string | null;
  onFeedSelect?: (feedId: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds
const QUERY_DEBOUNCE_TIME_MS = 300;
const ALL_SOURCE_TYPES = Object.values(SourceType);

interface CollapsedFeedSearchProps {
  feeds: Feed[];
  selectedFeed: Feed | null;
  localInputValue: string;
  setLocalInputValue: (value: string) => void;
  isFocused: boolean;
  setIsFocused: (focused: boolean) => void;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
  feedsLoading: boolean;
  tags: { key: string; value: string }[];
  onFeedSelect?: (feedId: string) => void;
}

function CollapsedFeedSearch({
  feeds,
  selectedFeed,
  localInputValue,
  setLocalInputValue,
  isFocused,
  setIsFocused,
  filters,
  onFiltersChange,
  feedsLoading,
  tags,
  onFeedSelect,
}: CollapsedFeedSearchProps) {
  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        alignItems: 'center',
        flexWrap: 'wrap',
        gap: 1.5,
      }}
    >
      <Autocomplete
        disablePortal
        options={feeds}
        filterOptions={(x) => x}
        getOptionLabel={(option) => option.name}
        size="small"
        sx={{
          flexGrow: 1,
          width: { xs: '100%', md: 'calc(50% - 6px)' },
          maxWidth: { md: 'calc(50% - 6px)' },
        }}
        value={selectedFeed}
        inputValue={localInputValue}
        onChange={(_, option) => {
          if (option) {
            onFeedSelect?.(option.id);
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
        renderInput={(params) => (
          <TextField
            {...params}
            label="Select feed"
            placeholder="Search or select feed..."
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

      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1.5,
          flexWrap: 'wrap',
          flexGrow: 1,
          width: { xs: '100%', md: 'calc(50% - 6px)' },
          maxWidth: { md: 'calc(50% - 6px)' },
        }}
      >
        <TuneIcon color="action" fontSize="small" />
        <Box sx={{ flexGrow: 1, minWidth: 120, maxWidth: { sm: 200 } }}>
          <MultiSelectFilter
            label="Source Type"
            options={ALL_SOURCE_TYPES}
            value={filters.sourceTypes}
            onChange={(types) =>
              onFiltersChange({ ...filters, sourceTypes: types })
            }
            size="small"
          />
        </Box>
        <Box sx={{ flexGrow: 1, minWidth: 120, maxWidth: { sm: 160 } }}>
          <MultiSelectFilter
            label="Status"
            options={['Active', 'Inactive', 'Error']}
            value={filters.statuses}
            onChange={(statuses) => onFiltersChange({ ...filters, statuses })}
            size="small"
          />
        </Box>
        <Box sx={{ flexGrow: 1, minWidth: 120, maxWidth: { sm: 200 } }}>
          <MultiSelectFilter
            label="Tags"
            options={tags}
            value={filters.tags}
            onChange={(tags) => onFiltersChange({ ...filters, tags })}
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
      </Box>
    </Box>
  );
}

interface ExpandedFeedSearchProps {
  title: string;
  feeds: Feed[];
  allFeeds: Feed[];
  feedsLoading: boolean;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
}

function ExpandedFeedSearch({
  title,
  feeds,
  allFeeds,
  feedsLoading,
  filters,
  onFiltersChange,
}: ExpandedFeedSearchProps) {
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
        allFeeds={allFeeds}
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
  collapsed = false,
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

  const [prevSelectedFeed, setPrevSelectedFeed] = useState<Feed | null>(
    selectedFeed
  );
  const [localInputValue, setLocalInputValue] = useState(
    selectedFeed?.name || ''
  );
  const [isFocused, setIsFocused] = useState(false);

  if (selectedFeed?.id !== prevSelectedFeed?.id) {
    setPrevSelectedFeed(selectedFeed);
    setLocalInputValue(selectedFeed?.name || '');
  }

  const sortedFeedsForAutocomplete = useMemo(() => {
    return [...(feeds ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

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

  if (collapsed) {
    return (
      <CollapsedFeedSearch
        feeds={sortedFeedsForAutocomplete}
        selectedFeed={selectedFeed}
        localInputValue={localInputValue}
        setLocalInputValue={setLocalInputValue}
        isFocused={isFocused}
        setIsFocused={setIsFocused}
        filters={filters}
        onFiltersChange={setFilters}
        feedsLoading={feedsLoading}
        tags={tags}
        onFeedSelect={onFeedSelect}
      />
    );
  }

  return (
    <ExpandedFeedSearch
      title={title}
      feeds={feeds ?? []}
      allFeeds={allFeeds}
      feedsLoading={feedsLoading}
      filters={filters}
      onFiltersChange={setFilters}
    />
  );
}

export default FeedSearchView;
