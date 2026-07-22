import { useEffect, useMemo, useState } from 'react';

import {
  Autocomplete,
  Box,
  Chip,
  CircularProgress,
  TextField,
  Typography,
} from '@mui/material';
import { type Feed, SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useFeedSearchOptions } from '../../hooks/useFeedSearchOptions';
import { useFeeds } from '../../hooks/useFeeds';
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
  onFeedSelect?: (feedId: string) => void;
}

interface CondensedFeedSearchResultsProps {
  feeds: Feed[];
  filters: FeedFilters;
  onFiltersChange: (filters: FeedFilters) => void;
  feedsLoading: boolean;
  feedTotal: number;
  onFeedSelect?: (feedId: string) => void;
  hasNextPage?: boolean;
  isFetchingNextPage?: boolean;
  onLoadMore?: () => void;
}

function CondensedFeedSearchResults({
  feeds,
  filters,
  onFiltersChange,
  feedsLoading,
  onFeedSelect,
  hasNextPage,
  isFetchingNextPage,
  onLoadMore,
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
        loading={feedsLoading || isFetchingNextPage}
        loadingText={
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CircularProgress size={16} /> Loading feeds...
          </Box>
        }
        slotProps={{
          listbox: {
            onScroll: (event: React.UIEvent<HTMLUListElement>) => {
              const listboxNode = event.currentTarget;
              if (
                listboxNode.scrollTop + listboxNode.clientHeight >=
                listboxNode.scrollHeight - 20
              ) {
                if (hasNextPage && !isFetchingNextPage && onLoadMore) {
                  onLoadMore();
                }
              }
            },
          },
        }}
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
  hasNextPage?: boolean;
  isFetchingNextPage?: boolean;
  onLoadMore?: () => void;
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
  hasNextPage,
  isFetchingNextPage,
  onLoadMore,
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
        hasNextPage={hasNextPage}
        isFetchingNextPage={isFetchingNextPage}
        onLoadMore={onLoadMore}
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

  const {
    feeds,
    total: feedTotal,
    error: feedsError,
    isLoading: feedsLoading,
    isFetchingNextPage,
    hasNextPage,
    fetchNextPage,
  } = useFeeds({
    token,
    searchQuery: filters.searchQuery,
    sourceTypes: filters.sourceTypes,
    statuses: filters.statuses,
    tags: filters.tags,
  });

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
        onFeedSelect={onFeedSelect}
        hasNextPage={hasNextPage}
        isFetchingNextPage={isFetchingNextPage}
        onLoadMore={fetchNextPage}
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
      hasNextPage={hasNextPage}
      isFetchingNextPage={isFetchingNextPage}
      onLoadMore={fetchNextPage}
    />
  );
}

export default FeedSearchView;
