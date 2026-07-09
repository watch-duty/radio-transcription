import { useState } from 'react';

import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import TagIcon from '@mui/icons-material/Tag';
import Alert from '@mui/material/Alert';
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
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  BackendFeedStatus,
  FeedCreate,
  FeedStatus,
  FeedUpdate,
  Tag,
} from '@transcription/common';
import { SourceType } from '@transcription/common';

import { toSourceTypeString } from '../../utils/textUtils';
import {
  type TagKeyLimit,
  isValidTimezone,
  tagAddError,
  validateFeedSourceId,
  validateTags,
} from '../../utils/validationUtils';
import {
  ConfirmationDialog,
  type ConfirmationDialogProps,
} from '../common/ConfirmationDialog';
import { type TagRow, nextTagRowId } from './tagRows';

const ALL_SOURCE_TYPES = Object.values(SourceType).map((value) => {
  return { value, label: toSourceTypeString(value) };
});

const SYSTEM_TIMEZONE = 'system/timezone';

const ALL_TIMEZONES = Array.from(
  // Some older browsers might not support UTC in this list.
  new Set([...Intl.supportedValuesOf('timeZone'), 'UTC'])
).sort((a, b) => {
  if (a === 'UTC') return -1;
  if (b === 'UTC') return 1;

  const aIsAmerica = a.startsWith('America/');
  const bIsAmerica = b.startsWith('America/');
  if (aIsAmerica && !bIsAmerica) return -1;
  if (!aIsAmerica && bIsAmerica) return 1;

  return a.localeCompare(b);
});

// Keys default to multi-value + free-text (e.g. `region` for multi-county
// feeds); only exceptions need an entry here. `options` also renders a dropdown.
interface TagKeyConfig extends TagKeyLimit {
  valueLabel?: string; // dropdown label (defaults to "Value")
}

const TAG_KEY_CONFIG: Record<string, TagKeyConfig> = {
  // system/timezone is currently only recognized by the Fire Notifications collector.
  [SYSTEM_TIMEZONE]: {
    maxValues: 1,
    options: ALL_TIMEZONES,
    // Accept any IANA zone (incl. aliases like US/Pacific) the dropdown omits, so
    // editing a feed whose timezone was set outside this UI doesn't block saving.
    validate: isValidTimezone,
    valueLabel: 'Timezone',
  },
};

const tagKeyConfig = (key: string): TagKeyConfig | undefined =>
  TAG_KEY_CONFIG[key.trim()];

export const DialogType = {
  Delete: 'delete',
  Reset: 'reset',
  Deactivate: 'deactivate',
} as const;

export type DialogType = (typeof DialogType)[keyof typeof DialogType];

const DIALOG_CONFIG: Record<
  DialogType,
  {
    title: string;
    description: string;
    confirmLabel: string;
    confirmColor: ConfirmationDialogProps['confirmColor'];
    showConfirmInput: boolean;
  }
> = {
  [DialogType.Delete]: {
    title: 'Verify Feed Deletion',
    description:
      'Are you sure you want to delete the feed? This will remove the feed and any associated metadata (e.g. transcripts, annotations, etc.). This action is not reversible.',
    confirmLabel: 'Delete',
    confirmColor: 'error',
    showConfirmInput: true,
  },
  [DialogType.Reset]: {
    title: 'Verify Feed Reset',
    description:
      'Are you sure you want to reset this feed? This will re-enable the feed processing.',
    confirmLabel: 'Reset',
    confirmColor: 'primary',
    showConfirmInput: false,
  },
  [DialogType.Deactivate]: {
    title: 'Verify Feed Deactivation',
    description:
      'Are you sure you want to deactivate this feed? Feed processing will stop until the feed is explicitly reset.',
    confirmLabel: 'Deactivate',
    confirmColor: 'error',
    showConfirmInput: false,
  },
};

interface FeedConfigurationEditProps {
  isEditing: boolean;
  feedName: string;
  feedSourceType: SourceType;
  feedSourceId: string;
  feedTags: TagRow[];
  feedStatus?: FeedStatus;
  feedSubstatus?: BackendFeedStatus;
  setFeedName: (name: string) => void;
  setFeedSourceType: (sourceType: SourceType) => void;
  setFeedSourceId: (sourceFeedId: string) => void;
  setFeedTags: (tags: TagRow[]) => void;
  onCreateFeed: (payload: FeedCreate) => Promise<void>;
  onUpdateFeed: (payload: FeedUpdate) => Promise<void>;
  /** Callback triggered to hard delete the feed. If undefined, "Delete feed" is hidden from the actions menu. */
  onDeleteFeed?: () => Promise<void>;
  /** Callback triggered to deactivate the feed. If undefined, "Deactivate feed" is hidden from the actions menu. */
  onDeactivateFeed?: () => Promise<void>;
  /** Callback triggered to reset/re-enable the feed. If undefined, "Reset feed" is hidden from the actions menu. */
  onResetFeed?: () => Promise<void>;
  onCancel: () => void;
  isSubmitting: boolean;
}

export function FeedConfigurationEdit({
  isEditing,
  feedName,
  feedSourceType,
  feedSourceId,
  feedTags,
  feedStatus,
  feedSubstatus,
  setFeedName,
  setFeedSourceType,
  setFeedSourceId,
  setFeedTags,
  onCreateFeed,
  onUpdateFeed,
  onDeleteFeed,
  onDeactivateFeed,
  onResetFeed,
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

  const [activeDialog, setActiveDialog] = useState<DialogType | null>(null);

  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const menuOpen = Boolean(menuAnchorEl);

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setMenuAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchorEl(null);
  };

  const handleDeleteClick = () => {
    handleMenuClose();
    setActiveDialog(DialogType.Delete);
  };

  const handleResetClick = () => {
    handleMenuClose();
    setActiveDialog(DialogType.Reset);
  };

  const handleDeactivateClick = () => {
    handleMenuClose();
    setActiveDialog(DialogType.Deactivate);
  };

  const handleDialogClose = () => {
    setActiveDialog(null);
  };

  const handleConfirmAction = async () => {
    const currentDialog = activeDialog;
    setActiveDialog(null);
    if (currentDialog === DialogType.Delete && onDeleteFeed) {
      await onDeleteFeed();
    } else if (currentDialog === DialogType.Reset && onResetFeed) {
      await onResetFeed();
    } else if (currentDialog === DialogType.Deactivate && onDeactivateFeed) {
      await onDeactivateFeed();
    }
  };

  const handleKeyChange = (val: string) => {
    const trimmedVal = val.trim();
    setNewTagKey(val);

    // Clear the value when switching to an enum key whose current value isn't allowed.
    const allowedOptions = tagKeyConfig(trimmedVal)?.options;
    if (allowedOptions && !allowedOptions.includes(newTagValue)) {
      setNewTagValue('');
    }

    setValidationErrors((prev) => {
      if (!prev.tags) return prev;
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  const handleValueChange = (val: string) => {
    setNewTagValue(val);
    setValidationErrors((prev) => {
      if (!prev.tags) return prev;
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  const resetFormState = () => {
    setFeedName('');
    setFeedSourceType(SourceType.BCFY_FEEDS);
    setFeedSourceId('');
    setFeedTags([]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors({});
    setActiveDialog(null);
  };

  // Tag interactions
  const handleAddTag = () => {
    const key = newTagKey.trim();
    const value = newTagValue.trim();

    // If both fields are completely blank, they didn't input anything.
    // Do not show a validation error, just ignore the click and return early.
    if (!key && !value) {
      return;
    }

    if (!key || !value) {
      setValidationErrors((prev) => ({
        ...prev,
        tags: 'Both key and value must be populated to add a tag.',
      }));
      return;
    }

    const addError = tagAddError(
      feedTags,
      key,
      value,
      tagKeyConfig(key)?.maxValues
    );
    if (addError) {
      setValidationErrors((prev) => ({ ...prev, tags: addError }));
      return;
    }

    setFeedTags([...feedTags, { id: nextTagRowId(), key, value }]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  const handleRemoveTag = (idToRemove: string) => {
    setFeedTags(feedTags.filter((tag) => tag.id !== idToRemove));
  };

  const handleUpdateTag = (
    id: string,
    field: 'key' | 'value',
    newValue: string
  ) => {
    const updated = feedTags.map((tag) => {
      if (tag.id !== id) return tag;
      const next = { ...tag, [field]: newValue };

      // Clear the value when switching to an enum key whose current value isn't allowed.
      if (field === 'key') {
        const allowedOptions = tagKeyConfig(newValue)?.options;
        if (allowedOptions && !allowedOptions.includes(next.value)) {
          next.value = '';
        }
      }
      return next;
    });

    setFeedTags(updated);
    setValidationErrors((prev) => {
      const copy = { ...prev };
      delete copy.tags;
      return copy;
    });
  };

  // Local schema verification before dispatching mutations
  const validateForm = (
    tagsToValidate: Tag[],
    inProgressTag: { key: string; value: string }
  ): Record<string, string> => {
    const errors: Record<string, string> = {};

    if (!feedName.trim()) {
      errors.name = 'Display name is required.';
    }

    const sourceIdError = validateFeedSourceId(feedSourceType, feedSourceId);
    if (sourceIdError) {
      errors.sourceFeedId = sourceIdError;
    }

    const tagError = validateTags(
      tagsToValidate,
      inProgressTag,
      TAG_KEY_CONFIG
    );
    if (tagError) {
      errors.tags = tagError;
    }

    return errors;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    const inProgressTag = { key: newTagKey, value: newTagValue };
    const errors = validateForm(feedTags, inProgressTag);

    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setValidationErrors({});

    // Build the final rows to submit. If there was a valid in-progress tag, include it.
    const trimmedNewKey = newTagKey.trim();
    const trimmedNewValue = newTagValue.trim();
    const finalRows = [...feedTags];
    if (trimmedNewKey && trimmedNewValue) {
      finalRows.push({
        id: nextTagRowId(),
        key: trimmedNewKey,
        value: trimmedNewValue,
      });
      // Update state so the UI reflects the added tag and clears the inputs
      setFeedTags(finalRows);
      setNewTagKey('');
      setNewTagValue('');
    }

    // Strip the client-side row id before sending to the API.
    const finalTags: Tag[] = finalRows.map(({ key, value }) => ({
      key,
      value,
    }));

    try {
      if (isEditing) {
        const payload: FeedUpdate = {
          name: feedName.trim(),
          tags: finalTags,
        };
        await onUpdateFeed(payload);
      } else {
        const payload: FeedCreate = {
          name: feedName.trim(),
          sourceType: feedSourceType,
          sourceFeedId: feedSourceId.trim(),
          tags: finalTags,
        };
        await onCreateFeed(payload);
        resetFormState();
      }
    } catch {
      // Errors are typically caught and propagated in Mutate onError side-effects
    }
  };

  const dialogConfig = activeDialog ? DIALOG_CONFIG[activeDialog] : null;
  const newTagConfig = tagKeyConfig(newTagKey);
  const newTagValueLabel = newTagConfig?.valueLabel ?? 'Value';
  const tagRows = feedTags.map((tag, index) => {
    const config = tagKeyConfig(tag.key);
    return { tag, index, config, valueLabel: config?.valueLabel ?? 'Value' };
  });

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
                <FormControl disabled={!!isEditing || isSubmitting}>
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
                    {ALL_SOURCE_TYPES.map((opt) => (
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
              <Alert severity="info" sx={{ mb: 2 }}>
                The "system/timezone" tag can be used to correct the timestamps.
                This is only supported in Fire Notification feeds.
              </Alert>

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
                  onChange={(e) => handleKeyChange(e.target.value)}
                  error={!!validationErrors.tags}
                  disabled={isSubmitting}
                  sx={{ flex: 1 }}
                />
                {newTagConfig?.options ? (
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel id="enum-tag-label">
                      {newTagValueLabel}
                    </InputLabel>
                    <Select
                      labelId="enum-tag-label"
                      id="enum-tag-dropdown"
                      value={newTagValue}
                      label={newTagValueLabel}
                      onChange={(e) => handleValueChange(e.target.value)}
                      error={!!validationErrors.tags}
                      disabled={isSubmitting}
                      fullWidth
                    >
                      {newTagConfig.options.map((option) => (
                        <MenuItem key={option} value={option}>
                          {option}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : (
                  <TextField
                    size="small"
                    label="Value"
                    placeholder="Ventura"
                    value={newTagValue}
                    onChange={(e) => handleValueChange(e.target.value)}
                    error={!!validationErrors.tags}
                    disabled={isSubmitting}
                    sx={{ flex: 1 }}
                  />
                )}
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
                  tagRows.map(({ tag, index, config, valueLabel }) => (
                    <Stack
                      key={tag.id}
                      direction="row"
                      spacing={1.5}
                      sx={{ alignItems: 'center' }}
                    >
                      <TextField
                        size="small"
                        label="Key"
                        value={tag.key}
                        onChange={(e) => {
                          const newKey = e.target.value;
                          handleUpdateTag(tag.id, 'key', newKey);
                        }}
                        disabled={isSubmitting}
                        sx={{ flex: 1 }}
                      />
                      {config?.options ? (
                        <FormControl size="small" sx={{ flex: 1 }}>
                          <InputLabel id={`enum-tag-label-${tag.id}`}>
                            {valueLabel}
                          </InputLabel>
                          <Select
                            labelId={`enum-tag-label-${tag.id}`}
                            id={`enum-tag-dropdown-${tag.id}`}
                            value={tag.value}
                            label={valueLabel}
                            onChange={(e) =>
                              handleUpdateTag(tag.id, 'value', e.target.value)
                            }
                            disabled={isSubmitting}
                            fullWidth
                          >
                            {config.options.map((option) => (
                              <MenuItem key={option} value={option}>
                                {option}
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                      ) : (
                        <TextField
                          size="small"
                          label="Value"
                          value={tag.value}
                          onChange={(e) =>
                            handleUpdateTag(tag.id, 'value', e.target.value)
                          }
                          disabled={isSubmitting}
                          sx={{ flex: 1 }}
                        />
                      )}
                      <IconButton
                        size="small"
                        onClick={() => handleRemoveTag(tag.id)}
                        disabled={isSubmitting}
                        color="error"
                        aria-label={`Remove tag ${index + 1}: ${tag.key}=${tag.value}`}
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
                alignItems: 'center',
                width: '100%',
                mt: 1,
                gap: 2,
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

              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
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

                {isEditing && (
                  <>
                    <IconButton
                      aria-label="feed actions"
                      aria-controls={menuOpen ? 'feed-actions-menu' : undefined}
                      aria-haspopup="true"
                      aria-expanded={menuOpen ? 'true' : undefined}
                      onClick={handleMenuOpen}
                      disabled={isSubmitting}
                      size="small"
                    >
                      <MoreVertIcon />
                    </IconButton>
                    <Menu
                      id="feed-actions-menu"
                      anchorEl={menuAnchorEl}
                      open={menuOpen}
                      onClose={handleMenuClose}
                      transformOrigin={{
                        vertical: 'top',
                        horizontal: 'right',
                      }}
                      anchorOrigin={{
                        vertical: 'bottom',
                        horizontal: 'right',
                      }}
                    >
                      {onResetFeed &&
                        feedStatus &&
                        feedStatus !== 'active' &&
                        feedSubstatus &&
                        feedSubstatus !== 'unclaimed' && (
                          <MenuItem
                            onClick={handleResetClick}
                            disabled={isSubmitting}
                          >
                            Reset feed
                          </MenuItem>
                        )}
                      {onDeactivateFeed &&
                        feedSubstatus &&
                        feedSubstatus !== 'deactivated' && (
                          <MenuItem
                            onClick={handleDeactivateClick}
                            disabled={isSubmitting}
                          >
                            Deactivate feed
                          </MenuItem>
                        )}
                      <MenuItem
                        onClick={handleDeleteClick}
                        disabled={isSubmitting}
                        sx={{ color: 'error.main' }}
                      >
                        Delete feed
                      </MenuItem>
                    </Menu>
                  </>
                )}
              </Box>
            </Box>
          </Stack>
        </Box>
      </CardContent>

      {dialogConfig && (
        <ConfirmationDialog
          open={activeDialog !== null}
          title={dialogConfig.title}
          description={dialogConfig.description}
          confirmLabel={dialogConfig.confirmLabel}
          confirmColor={dialogConfig.confirmColor}
          showConfirmInput={dialogConfig.showConfirmInput}
          confirmInputValue={feedSourceId}
          confirmInputLabel={`To confirm, type the Source Feed ID "${feedSourceId}" below:`}
          onClose={handleDialogClose}
          onConfirm={handleConfirmAction}
          isSubmitting={isSubmitting}
        />
      )}
    </Card>
  );
}

export default FeedConfigurationEdit;
