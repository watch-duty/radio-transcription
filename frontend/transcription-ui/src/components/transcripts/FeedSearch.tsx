import { useEffect, useMemo, useState } from 'react';

import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';

export interface FeedOption {
  id: string;
  name: string;
  externalId?: string;
}

export interface FeedSearchProps {
  feeds: FeedOption[];
  selectedFeed: FeedOption | null;
  onFeedSelect: (feedId: string) => void;
  isFetching: boolean;
  isLoading: boolean;
}

export function FeedSearch({
  feeds,
  selectedFeed,
  onFeedSelect,
  isFetching,
  isLoading,
}: FeedSearchProps) {
  const [inputValue, setInputValue] = useState('');

  // Synchronize the displayed input text whenever the selection changes externally
  // (e.g., initial load from a URL query parameter, clicking a deep link, or browser back/forward navigation)
  useEffect(() => {
    setInputValue(selectedFeed ? selectedFeed.name : '');
  }, [selectedFeed?.id]); // Use the ID primitive so background data polling updates don't reset input text while typing

  const sortedFeeds = useMemo(() => {
    return [...(feeds ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

  const filteredOptions = useMemo(() => {
    const search = inputValue.trim().toLowerCase();
    if (!search) return sortedFeeds;

    return sortedFeeds.filter((feed) =>
      feed.id.toLowerCase().includes(search) ||
      feed.name.toLowerCase().includes(search) ||
      (feed.externalId && feed.externalId.toLowerCase().includes(search))
    );
  }, [sortedFeeds, inputValue]);

  return (
    <Autocomplete
      disablePortal
      options={sortedFeeds}
      filterOptions={() => filteredOptions}
      getOptionLabel={(option) => option.name}
      size="small"
      sx={{ width: '20%' }}
      value={selectedFeed}
      onChange={(_, option) => {
        const nextId = option ? option.id : '';
        onFeedSelect(nextId);
        setInputValue(option ? option.name : '');
      }}
      inputValue={inputValue}
      onInputChange={(event, nextInputValue, reason) => {
        // Safeguard against background polling updates:
        // 'reset' is fired internally by MUI whenever state or options update.
        // If 'event' is defined/truthy, this signifies a real user action (dropdown selection).
        // Programmatic updates pass event=null, which we intentionally ignore to avoid overriding active typing.
        if (reason === 'input' || reason === 'clear' || (reason === 'reset' && event)) {
          setInputValue(nextInputValue);
        }
      }}
      freeSolo={false}
      loading={isFetching}
      disabled={isLoading}
      renderInput={(params) => (
        <TextField {...params} label="Select a registered feed" />
      )}
      renderOption={(props, option) => {
        const { key, ...optionProps } = props;
        return (
          <Box key={key} component="li" {...optionProps}>
            <Typography noWrap>{option.name}</Typography>
          </Box>
        );
      }}
    />
  );
}

export default FeedSearch;
