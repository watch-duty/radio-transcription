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
import { type Feed, SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useFeedSearchOptions } from '../../hooks/useFeedSearchOptions';
import { listFeeds } from '../../service/listFeeds';
import { toSourceTypeString } from '../../utils/textUtils';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';
import { type FeedFilters, FeedTable } from './FeedTable';
import { FeedTagChip } from './FeedTagChip';
import { groupTagsByKey } from './tagDisplay';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
  condensed?: boolean;
  // Condensed selector fills its container instead of the default half-width.
  fullWidth?: boolean;
  // Keep the chosen feed in the field; the consumer controls/reset it via `value`.
  retainSelection?: boolean;
  value?: Feed | null;
  onFeedSelect?: (feed: Feed) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds
const QUERY_DEBOUNCE_TIME_MS = 300;

interface CondensedFeedSearchResultsProps {
  feeds: Feed[];
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
  feedsLoading: boolean;
  feedTotal: number;
  fullWidth?: boolean;
  // Keep the chosen feed shown in the field (controlled by `value`) instead of
  // clearing on select. The consumer resets it by setting `value` back to null.
  retainSelection?: boolean;
  value?: Feed | null;
  onFeedSelect?: (feed: Feed) => void;
}

function CondensedFeedSearchResults({
  feeds,
  filters,
  onFiltersChange,
  feedsLoading,
  fullWidth,
  retainSelection,
  value,
  onFeedSelect,
}: CondensedFeedSearchResultsProps) {
  const [inputValue, setInputValue] = useState('');

  return (
    <Box
      sx={{
        textAlign: 'left',
        width: fullWidth ? 'auto' : '50%',
        flexGrow: fullWidth ? 1 : 0,
        minWidth: 0,
      }}
    >
      <Autocomplete
        disablePortal
        options={feeds}
        // Prevents client-side filtering since all filtering is done server-side.
        filterOptions={(x) => x}
        getOptionLabel={(option) => option.name}
        isOptionEqualToValue={(option, selected) => option.id === selected.id}
        size="small"
        value={retainSelection ? (value ?? null) : null}
        // In retain mode the field text is derived from the controlled `value`
        // (shows the picked name, clears when the consumer resets it); otherwise
        // it's a controlled search box that clears on select.
        inputValue={retainSelection ? undefined : inputValue}
        onChange={(_, option) => {
          if (option && onFeedSelect) {
            onFeedSelect(option);
            if (!retainSelection) {
              setInputValue('');
              onFiltersChange({ ...filters, searchQuery: '' });
            }
          }
        }}
        onInputChange={(_, newInputValue, reason) => {
          if (!retainSelection) {
            setInputValue(newInputValue);
            onFiltersChange({ ...filters, searchQuery: newInputValue });
          } else if (reason === 'input') {
            // Only user typing drives the server search, not the value → text sync.
            onFiltersChange({ ...filters, searchQuery: newInputValue });
          }
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
            placeholder="Search or select feed..."
            slotProps={{
              ...params.slotProps,
              htmlInput: {
                ...params.slotProps?.htmlInput,
                'aria-label': 'Select feed',
              },
            }}
            sx={{
              '& .MuiOutlinedInput-root': {
                height: 36,
                borderRadius: 1,
              },
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
                  lastSpeechSegmentTimestamp={option.lastSpeechSegmentTimestamp}
                  statusReason={option.statusReason}
                  statusReasonDetail={option.statusReasonDetail}
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
                    {groupTagsByKey(option.tags).map((group) => (
                      <FeedTagChip
                        key={`feed-${option.id}-tag-${group.key}`}
                        group={group}
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
  sourceTypes?: SourceType[];
  statuses?: string[];
  feedsLoading: boolean;
  feedTotal: number;
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
}

function TableFeedSearchResults({
  title,
  feeds,
  tags,
  sourceTypes,
  statuses,
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
        sourceTypes={sourceTypes}
        statuses={statuses}
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
  fullWidth = false,
  retainSelection = false,
  value,
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

  const { data: searchOptionsData } = useFeedSearchOptions(token);

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  const tags = searchOptionsData?.tags ?? [];

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
        fullWidth={fullWidth}
        retainSelection={retainSelection}
        value={value}
        onFeedSelect={onFeedSelect}
      />
    );
  }

  return (
    <TableFeedSearchResults
      title={title}
      feeds={feeds ?? []}
      tags={tags}
      sourceTypes={searchOptionsData?.sourceTypes}
      statuses={searchOptionsData?.statuses}
      feedsLoading={feedsLoading}
      feedTotal={feedTotal}
      filters={filters}
      onFiltersChange={setFilters}
    />
  );
}

export default FeedSearchView;
