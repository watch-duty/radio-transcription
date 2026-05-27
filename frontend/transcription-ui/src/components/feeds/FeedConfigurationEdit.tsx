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
import { useTheme } from '@mui/material/styles';
import type {
  Feed,
  FeedCreate,
  FeedUpdate,
  SourceType,
  Tag,
} from '@transcription/common';

const SOURCE_TYPE_OPTIONS: {
  value: SourceType;
  label: string;
}[] = [
  {
    value: 'bcfy_feeds',
    label: 'Broadcastify Feeds',
  },
  {
    value: 'bcfy_calls',
    label: 'Broadcastify Calls',
  },
  {
    value: 'openmhz',
    label: 'OpenMHZ',
  },
  {
    value: 'echo',
    label: 'Echo',
  },
  {
    value: 'fire_notifications',
    label: 'Fire Notifications',
  },
];

interface FeedConfigurationEditProps {
  editingFeed: Feed | null;
  onCreateFeed: (payload: FeedCreate) => Promise<void>;
  onUpdateFeed: (feedId: string, payload: FeedUpdate) => Promise<void>;
  onCancel: () => void;
  isSubmitting: boolean;
}

export function FeedConfigurationEdit({
  editingFeed,
  onCreateFeed,
  onUpdateFeed,
  onCancel,
  isSubmitting,
}: FeedConfigurationEditProps) {
  const theme = useTheme();

  // Track previous editingFeed to adjust local state during render when prop updates
  const [prevEditingFeed, setPrevEditingFeed] = useState<Feed | null>(null);

  // Form Fields
  const [name, setName] = useState('');
  const [sourceType, setSourceType] = useState<SourceType>('bcfy_feeds');
  const [sourceFeedId, setSourceFeedId] = useState('');
  const [tags, setTags] = useState<Tag[]>([]);

  // Subform dynamic fields for Tags
  const [newTagKey, setNewTagKey] = useState('');
  const [newTagValue, setNewTagValue] = useState('');

  // Local validation error states
  const [validationErrors, setValidationErrors] = useState<
    Record<string, string>
  >({});

  const resetFormState = () => {
    setName('');
    setSourceType('bcfy_feeds');
    setSourceFeedId('');
    setTags([]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors({});
  };

  // Adjust local states during rendering when the target editingFeed changes
  if (editingFeed !== prevEditingFeed) {
    setPrevEditingFeed(editingFeed);

    if (editingFeed) {
      setName(editingFeed.name);
      setSourceType(editingFeed.sourceType);
      setSourceFeedId(editingFeed.sourceFeedId || '');
      setTags(editingFeed.tags || []);
    } else {
      setName('');
      setSourceType('bcfy_feeds');
      setSourceFeedId('');
      setTags([]);
    }
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors({});
  }

  // Tag interactions
  const handleAddTag = () => {
    const key = newTagKey.trim().toLowerCase();
    const value = newTagValue.trim();

    if (!key || !value) {
      setValidationErrors((prev) => ({
        ...prev,
        tags: 'Both key and value must be populated to add a tag.',
      }));
      return;
    }

    // Prevent duplicate keys in tags list
    if (tags.some((t) => t.key === key)) {
      setValidationErrors((prev) => ({
        ...prev,
        tags: `A tag with key "${key}" already exists.`,
      }));
      return;
    }

    setTags((prev) => [...prev, { key, value }]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  const handleRemoveTag = (keyToRemove: string) => {
    setTags((prev) => prev.filter((tag) => tag.key !== keyToRemove));
  };

  const handleUpdateTag = (
    index: number,
    field: 'key' | 'value',
    newValue: string
  ) => {
    setTags((prev) => {
      const copy = [...prev];
      if (field === 'key') {
        copy[index] = { ...copy[index], key: newValue.toLowerCase() };
      } else {
        copy[index] = { ...copy[index], value: newValue };
      }
      return copy;
    });
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  // Local schema verification before dispatching mutations
  const validateForm = (): boolean => {
    const errors: Record<string, string> = {};

    if (!name.trim()) {
      errors.name = 'Display name is required.';
    }

    // Source Feed ID and Source Type are only required when creating.
    // In update mode they are permanent, but we validate to stay completely defensive.
    if (!editingFeed) {
      if (!sourceFeedId.trim()) {
        errors.sourceFeedId = 'Source feed ID is required.';
      }
    }

    // Verify tags data integrity
    const duplicateKeys = tags.filter(
      (tag, idx) => tags.findIndex((t) => t.key === tag.key) !== idx
    );
    if (duplicateKeys.length > 0) {
      errors.tags = `Duplicate tag keys discovered: ${duplicateKeys
        .map((d) => d.key)
        .join(', ')}. Keys must be unique.`;
    }

    const blankTags = tags.some((tag) => !tag.key.trim() || !tag.value.trim());
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
      if (editingFeed) {
        const payload: FeedUpdate = {
          name: name.trim(),
          externalId: '',
          tags: tags.length > 0 ? tags : undefined,
        };
        await onUpdateFeed(editingFeed.id, payload);
      } else {
        const payload: FeedCreate = {
          name: name.trim(),
          sourceType,
          sourceFeedId: sourceFeedId.trim(),
          externalId: '',
          tags: tags.length > 0 ? tags : undefined,
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
          color: editingFeed
            ? theme.palette.warning.contrastText
            : theme.palette.primary.contrastText,
          background: editingFeed
            ? theme.palette.warning.main
            : theme.palette.primary.main,
          flexShrink: 0,
        }}
      >
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          {editingFeed ? 'Edit Feed' : 'Register New Feed'}
        </Typography>
        {editingFeed && (
          <Typography variant="body2" sx={{ opacity: 0.85, mt: 0.5 }}>
            Modifying configuration for {editingFeed.name}
          </Typography>
        )}
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
              value={name}
              onChange={(e) => setName(e.target.value)}
              error={!!validationErrors.name}
              helperText={
                validationErrors.name || 'Concise and readable name of the feed'
              }
              disabled={isSubmitting}
            />

            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>
                <FormControl fullWidth disabled={!!editingFeed || isSubmitting}>
                  <InputLabel id="source-type-select-label">
                    Source Type
                  </InputLabel>
                  <Select
                    labelId="source-type-select-label"
                    id="source-type-select"
                    value={sourceType}
                    label="Source Type"
                    size="small"
                    onChange={(e) =>
                      setSourceType(e.target.value as SourceType)
                    }
                  >
                    {SOURCE_TYPE_OPTIONS.map((opt) => (
                      <MenuItem key={opt.value} value={opt.value}>
                        {opt.label}
                      </MenuItem>
                    ))}
                  </Select>
                  <FormHelperText>
                    {editingFeed
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
                  value={sourceFeedId}
                  onChange={(e) => setSourceFeedId(e.target.value)}
                  error={!!validationErrors.sourceFeedId}
                  helperText={
                    validationErrors.sourceFeedId || 'Unique ID of the source'
                  }
                  disabled={!!editingFeed || isSubmitting}
                  slotProps={{
                    input: {
                      endAdornment: editingFeed ? (
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
                  label="Tag Key"
                  placeholder="county"
                  value={newTagKey}
                  onChange={(e) => setNewTagKey(e.target.value)}
                  error={!!validationErrors.tags}
                  disabled={isSubmitting}
                  sx={{ flexGrow: 1 }}
                />
                <TextField
                  size="small"
                  label="Tag Value"
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
                {tags.length === 0 ? (
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mx: 'auto', py: 2, fontStyle: 'italic' }}
                  >
                    No custom tags added.
                  </Typography>
                ) : (
                  tags.map((tag, index) => (
                    <Stack
                      key={index}
                      direction="row"
                      spacing={1.5}
                      sx={{ alignItems: 'center' }}
                    >
                      <TextField
                        size="small"
                        label="Tag Key"
                        value={tag.key}
                        onChange={(e) =>
                          handleUpdateTag(index, 'key', e.target.value)
                        }
                        disabled={isSubmitting}
                        sx={{ flexGrow: 1 }}
                      />
                      <TextField
                        size="small"
                        label="Tag Value"
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
              {editingFeed && (
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
                ) : editingFeed ? (
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
