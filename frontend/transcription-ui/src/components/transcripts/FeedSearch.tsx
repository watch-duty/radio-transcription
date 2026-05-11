import { useMemo } from 'react';

import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type { Feed } from '@transcription/common';

export interface FeedSearchProps {
  feeds: Feed[];
  selectedFeed: Feed | null;
  onFeedSelect: (feedId: string) => void;
  isFetching: boolean;
}

export function FeedSearch({
  feeds,
  selectedFeed,
  onFeedSelect,
  isFetching,
}: FeedSearchProps) {
  const sortedFeeds = useMemo(() => {
    return [...(feeds ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

  return (
    <Autocomplete
      disablePortal
      options={sortedFeeds}
      getOptionLabel={(option) => option.name}
      size="small"
      sx={{ width: '20%' }}
      value={selectedFeed}
      onChange={(_, option) => option && onFeedSelect(option.id)}
      // Explicitly disallowing custom input - the user should always pick from registered feeds
      freeSolo={false}
      loading={isFetching}
      disabled={isFetching}
      filterOptions={(options, { inputValue }) =>
        options.filter(
          (option) =>
            option.name.toLowerCase().includes(inputValue.toLowerCase()) ||
            option.id.toLowerCase().includes(inputValue.toLowerCase())
        )
      }
      renderInput={(params) => (
        <TextField {...params} label="Select a registered feed" />
      )}
      renderOption={(props, option) => {
        const { key, ...optionProps } = props;
        return (
          <Box
            key={key}
            component="li"
            {...optionProps}
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              width: '100%',
            }}
          >
            <Typography noWrap>{option.name}</Typography>
            <Chip label={option.sourceType} size="small" sx={{ ml: 1 }} />
          </Box>
        );
      }}
    />
  );
}

export default FeedSearch;
