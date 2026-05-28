import { useState } from 'react';

import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import TagIcon from '@mui/icons-material/Tag';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import FormHelperText from '@mui/material/FormHelperText';
import Grid from '@mui/material/Grid';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { FeedCreate, FeedUpdate, Tag } from '@transcription/common';
import { SourceType } from '@transcription/common';

const SOURCE_TYPE_OPTIONS: {
  value: SourceType;
  label: string;
}[] = [
  {
    value: SourceType.BCFY_FEEDS,
    label: 'Broadcastify Feeds',
  },
  {
    value: SourceType.BCFY_CALLS,
    label: 'Broadcastify Calls',
  },
  {
    value: SourceType.OPENMHZ,
    label: 'OpenMHZ',
  },
  {
    value: SourceType.ECHO,
    label: 'Echo',
  },
  {
    value: SourceType.FIRE_NOTIFICATIONS,
    label: 'Fire Notifications',
  },
];

interface FeedConfigurationEditProps {
  isEditing: boolean;
  feedName: string;
  feedSourceType: SourceType;
  feedSourceId: string;
  feedTags: Tag[];
  setFeedName: (name: string) => void;
  setFeedSourceType: (sourceType: SourceType) => void;
  setFeedSourceId: (sourceFeedId: string) => void;
  setFeedTags: (tags: Tag[]) => void;
  onCreateFeed: (payload: FeedCreate) => Promise<void>;
  onUpdateFeed: (payload: FeedUpdate) => Promise<void>;
  onCancel: () => void;
  isSubmitting: boolean;
}

export function FeedConfigurationEdit({
  isEditing,
  feedName,
  feedSourceType,
  feedSourceId,
  feedTags,
  setFeedName,
  setFeedSourceType,
  setFeedSourceId,
  setFeedTags,
  onCreateFeed,
  onUpdateFeed,
  onCancel,
  isSubmitting,
}: FeedConfigurationEditProps) {
  // Subform dynamic fields for adding Tags
  const [newTagKey, setNewTagKey] = useState('');
  const [newTagValue, setNewTagValue] = useState('');

  // Local validation error states
  const [validationErrors, setValidationErrors] = useState<
    Record<string, string>
  >({});

  const resetFormState = () => {
    setFeedName('');
    setFeedSourceType(SourceType.BCFY_FEEDS);
    setFeedSourceId('');
    setFeedTags([]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors({});
  };

  // Tag interactions
  const handleAddTag = () => {
    const key = newTagKey.trim();
    const value = newTagValue.trim();

    if (!key || !value) {
      setValidationErrors((prev) => ({
        ...prev,
        tags: 'Both key and value must be populated to add a tag.',
      }));
      return;
    }

    // Prevent duplicate keys in tags list
    if (feedTags.some((t) => t.key === key)) {
      setValidationErrors((prev) => ({
        ...prev,
        tags: `A tag with key "${key}" already exists.`,
      }));
      return;
    }

    setFeedTags([...feedTags, { key, value }]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  const handleRemoveTag = (keyToRemove: string) => {
    setFeedTags(feedTags.filter((tag) => tag.key !== keyToRemove));
  };

  const handleUpdateTag = (
    index: number,
    field: 'key' | 'value',
    newValue: string
  ) => {
    const copy = [...feedTags];
    copy[index] = { ...copy[index], [field]: newValue };
    setFeedTags(copy);
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  // Local schema verification before dispatching mutations
  const validateForm = (): boolean => {
    const errors: Record<string, string> = {};

    if (!feedName.trim()) {
      errors.name = 'Display name is required.';
    }

    if (!feedSourceId.trim()) {
      errors.sourceFeedId = 'Source feed ID is required.';
    }

    // Verify tags data integrity
    const duplicateKeys = feedTags.filter(
      (tag, idx) => feedTags.findIndex((t) => t.key === tag.key) !== idx
    );
    if (duplicateKeys.length > 0) {
      errors.tags = `Duplicate tag keys discovered: ${duplicateKeys
        .map((d) => d.key)
        .join(', ')}. Keys must be unique.`;
    }

    const blankTags = feedTags.some(
      (tag) => !tag.key.trim() || !tag.value.trim()
    );
    if (blankTags) {
      errors.tags =
        'Tag key and value inputs cannot be blank. Discard empty tag rows using the delete button.';
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    try {
      if (isEditing) {
        const payload: FeedUpdate = {
          name: feedName.trim(),
          externalId: feedSourceId.trim(),
          tags: feedTags,
        };
        await onUpdateFeed(payload);
      } else {
        const payload: FeedCreate = {
          name: feedName.trim(),
          sourceType: feedSourceType,
          sourceFeedId: feedSourceId.trim(),
          externalId: feedSourceId.trim(),
          tags: feedTags,
        };
        await onCreateFeed(payload);
        resetFormState();
      }
    } catch {
      // Errors are typically caught and propagated in Mutate onError side-effects
    }
  };

  return (
    <Card
      variant="outlined"
      data-testid="feed-config-card"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      {/* Header with dynamic colors mapping create/edit mode state */}
      <Box
        sx={{
          p: 3,
          color: isEditing ? 'warning.contrastText' : 'primary.contrastText',
          bgcolor: isEditing ? 'warning.main' : 'primary.main',
          flexShrink: 0,
        }}
      >
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          {isEditing ? `Edit Feed: ${feedName}` : 'Register New Feed'}
        </Typography>
      </Box>

      <CardContent
        sx={{
          p: 3,
          display: 'flex',
          flexDirection: 'column',
          gap: 3,
          overflowY: 'auto',
          flexGrow: 1,
          minHeight: 0,
        }}
      >
        <Box component="form" onSubmit={handleSubmit} noValidate>
          <Stack spacing={3}>
            <TextField
              fullWidth
              label="Display Name"
              size="small"
              variant="outlined"
              placeholder="Ventura Public Safety - Fire Dispatch"
              value={feedName}
              onChange={(e) => setFeedName(e.target.value)}
              error={!!validationErrors.name}
              helperText={
                validationErrors.name || 'Concise and readable name of the feed'
              }
              disabled={isSubmitting}
            />

            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>
                <FormControl fullWidth disabled={!!isEditing || isSubmitting}>
                  <InputLabel id="source-type-select-label">
                    Source Type
                  </InputLabel>
                  <Select
                    labelId="source-type-select-label"
                    id="source-type-select"
                    value={feedSourceType}
                    label="Source Type"
                    size="small"
                    onChange={(e) =>
                      setFeedSourceType(e.target.value as SourceType)
                    }
                  >
                    {SOURCE_TYPE_OPTIONS.map((opt) => (
                      <MenuItem key={opt.value} value={opt.value}>
                        {opt.label}
                      </MenuItem>
                    ))}
                  </Select>
                  <FormHelperText>
                    {isEditing
                      ? 'Source cannot be changed after it has been registered'
                      : 'Source the audio comes from'}
                  </FormHelperText>
                </FormControl>
              </Grid>

              <Grid size={{ xs: 12, sm: 6 }}>
                <TextField
                  fullWidth
                  label="Source Feed ID"
                  variant="outlined"
                  size="small"
                  placeholder={'12345'}
                  value={feedSourceId}
                  onChange={(e) => setFeedSourceId(e.target.value)}
                  error={!!validationErrors.sourceFeedId}
                  helperText={
                    validationErrors.sourceFeedId || 'Unique ID of the source'
                  }
                  disabled={!!isEditing || isSubmitting}
                  slotProps={{
                    input: {
                      endAdornment: isEditing ? (
                        <InputAdornment position="end">
                          <Tooltip title="Source configs cannot be edited after initial mapping creation.">
                            <InfoOutlinedIcon
                              fontSize="small"
                              color="disabled"
                            />
                          </Tooltip>
                        </InputAdornment>
                      ) : undefined,
                    },
                  }}
                />
              </Grid>
            </Grid>

            <Divider sx={{ my: 1 }} />

            <Box>
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  mb: 1.5,
                }}
              >
                <TagIcon fontSize="small" color="action" />
                <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                  Tags
                </Typography>
              </Box>

              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ display: 'block', mb: 2 }}
              >
                Tags (e.g. county, agency, state) allow for better
                searchability, grouping, and routing of notifications.
              </Typography>

              <Stack
                direction="row"
                spacing={1.5}
                sx={{ mb: 2, alignItems: 'center' }}
              >
                <TextField
                  size="small"
                  label="Key"
                  placeholder="county"
                  value={newTagKey}
                  onChange={(e) => setNewTagKey(e.target.value)}
                  error={!!validationErrors.tags}
                  disabled={isSubmitting}
                  sx={{ flexGrow: 1 }}
                />
                <TextField
                  size="small"
                  label="Value"
                  placeholder="Ventura"
                  value={newTagValue}
                  onChange={(e) => setNewTagValue(e.target.value)}
                  error={!!validationErrors.tags}
                  disabled={isSubmitting}
                  sx={{ flexGrow: 1 }}
                />
                <Button
                  variant="outlined"
                  onClick={handleAddTag}
                  disabled={isSubmitting}
                  startIcon={<AddIcon fontSize="small" />}
                  sx={{
                    textTransform: 'none',
                  }}
                  aria-label="Add Tag"
                >
                  Add
                </Button>
              </Stack>

              {validationErrors.tags && (
                <Typography
                  variant="caption"
                  color="error"
                  sx={{ display: 'block', mb: 2 }}
                >
                  {validationErrors.tags}
                </Typography>
              )}

              {/* Tag list horizontal rows visualization */}
              <Box
                sx={{
                  p: 2,
                  borderRadius: 2.5,
                  border: '1px dashed',
                  borderColor: 'divider',
                  bgcolor: 'background.default',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 1.5,
                }}
              >
                {feedTags.length === 0 ? (
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mx: 'auto', py: 2, fontStyle: 'italic' }}
                  >
                    No tags added.
                  </Typography>
                ) : (
                  feedTags.map((tag, index) => (
                    <Stack
                      key={index}
                      direction="row"
                      spacing={1.5}
                      sx={{ alignItems: 'center' }}
                    >
                      <TextField
                        size="small"
                        label="Key"
                        value={tag.key}
                        onChange={(e) =>
                          handleUpdateTag(index, 'key', e.target.value)
                        }
                        disabled={isSubmitting}
                        sx={{ flexGrow: 1 }}
                      />
                      <TextField
                        size="small"
                        label="Value"
                        value={tag.value}
                        onChange={(e) =>
                          handleUpdateTag(index, 'value', e.target.value)
                        }
                        disabled={isSubmitting}
                        sx={{ flexGrow: 1 }}
                      />
                      <IconButton
                        size="small"
                        onClick={() => handleRemoveTag(tag.key)}
                        disabled={isSubmitting}
                        color="error"
                        aria-label={`Remove tag ${tag.key}`}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Stack>
                  ))
                )}
              </Box>
            </Box>

            <Box
              sx={{
                display: 'flex',
                justifyContent: 'flex-end',
                gap: 2,
                mt: 1,
              }}
            >
              {isEditing && (
                <Button
                  variant="outlined"
                  onClick={onCancel}
                  disabled={isSubmitting}
                  sx={{ textTransform: 'none' }}
                >
                  Cancel edit
                </Button>
              )}

              <Button
                type="submit"
                variant="contained"
                disabled={isSubmitting}
                sx={{ textTransform: 'none' }}
              >
                {isSubmitting ? (
                  <CircularProgress size={20} color="inherit" />
                ) : isEditing ? (
                  'Save changes'
                ) : (
                  'Register feed'
                )}
              </Button>
            </Box>
          </Stack>
        </Box>
      </CardContent>
    </Card>
  );
}

export default FeedConfigurationEdit;
