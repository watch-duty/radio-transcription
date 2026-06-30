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
  isValidTimezone,
  validateFeedSourceId,
} from '../../utils/validationUtils';
import {
  ConfirmationDialog,
  type ConfirmationDialogProps,
} from '../common/ConfirmationDialog';

const ALL_SOURCE_TYPES = Object.values(SourceType).map((value) => {
  return { value, label: toSourceTypeString(value) };
});

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
  feedTags: Tag[];
  feedStatus?: FeedStatus;
  feedSubstatus?: BackendFeedStatus;
  setFeedName: (name: string) => void;
  setFeedSourceType: (sourceType: SourceType) => void;
  setFeedSourceId: (sourceFeedId: string) => void;
  setFeedTags: (tags: Tag[]) => void;
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

    // Reset the tag value if the tag is intended to be a timezone since the possible
    // values are enums.
    if (trimmedVal === 'timezone') {
      if (!isValidTimezone(newTagValue)) {
        setNewTagValue('');
      }
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

    // Value needs to reset since only enums are allowed for timezone tags.
    if (field === 'key' && newValue === 'timezone') {
      copy[index].value = '';
    }

    setFeedTags(copy);
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

    // First check the in-progress tag inputs
    const trimmedNewKey = inProgressTag.key.trim();
    const trimmedNewValue = inProgressTag.value.trim();

    const combinedTags = [...tagsToValidate];

    // If there is something in the in-progress tag fields, validate it.
    if (trimmedNewKey && trimmedNewValue) {
      if (tagsToValidate.some((t) => t.key === trimmedNewKey)) {
        errors.tags = `A tag with key "${trimmedNewKey}" already exists.`;
      } else {
        combinedTags.push({ key: trimmedNewKey, value: trimmedNewValue });
      }
    } else if (trimmedNewKey || trimmedNewValue) {
      errors.tags = 'Both key and value must be populated to add a tag.';
    }

    // Validate timezone tag values
    // NOTE: The 'timezone' tag is currently only recognized by the Fire Notifications collector.
    for (const tag of combinedTags) {
      if (tag.key.trim() === 'timezone') {
        const tzValue = tag.value.trim();
        if (!isValidTimezone(tzValue)) {
          const validTzs = Intl.supportedValuesOf('timeZone');
          errors.tags = `Invalid timezone. Valid timezones: ${validTzs.join(', ')}`;
          break;
        }
      }
    }

    // Verify tags data integrity across the combined set
    const duplicateKeys = combinedTags.filter(
      (tag, idx) => combinedTags.findIndex((t) => t.key === tag.key) !== idx
    );
    if (duplicateKeys.length > 0) {
      errors.tags = `Duplicate tag keys discovered: ${duplicateKeys
        .map((d) => d.key)
        .join(', ')}. Keys must be unique.`;
    }

    const blankTags = combinedTags.some(
      (tag) => !tag.key.trim() || !tag.value.trim()
    );
    if (blankTags) {
      errors.tags =
        'Tag key and value inputs cannot be blank. Discard empty tag rows using the delete button.';
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

    // Build the final tags array to submit. If there was a valid in-progress tag, include it.
    const trimmedNewKey = newTagKey.trim();
    const trimmedNewValue = newTagValue.trim();
    const finalTags = [...feedTags];
    if (trimmedNewKey && trimmedNewValue) {
      finalTags.push({ key: trimmedNewKey, value: trimmedNewValue });
      // Update state so the UI reflects the added tag and clears the inputs
      setFeedTags(finalTags);
      setNewTagKey('');
      setNewTagValue('');
    }

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
                Timezone tags can be used to correct the timestamps, but this is
                only supported in Fire Notification feeds.
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
                {newTagKey.trim() === 'timezone' ? (
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel id="timezone-tag-label">Timezone</InputLabel>
                    <Select
                      labelId="timezone-tag-label"
                      id="timezone-tag-dropdown"
                      value={newTagValue}
                      label="Timezone"
                      onChange={(e) => handleValueChange(e.target.value)}
                      error={!!validationErrors.tags}
                      disabled={isSubmitting}
                      fullWidth
                    >
                      {ALL_TIMEZONES.map((tz) => (
                        <MenuItem key={tz} value={tz}>
                          {tz}
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
                        onChange={(e) => {
                          const newKey = e.target.value;
                          handleUpdateTag(index, 'key', newKey);
                        }}
                        disabled={isSubmitting}
                        sx={{ flex: 1 }}
                      />
                      {tag.key.trim() === 'timezone' ? (
                        <FormControl size="small" sx={{ flex: 1 }}>
                          <InputLabel id={`timezone-tag-label-${index}`}>
                            Timezone
                          </InputLabel>
                          <Select
                            labelId={`timezone-tag-label-${index}`}
                            id={`timezone-tag-dropdown-${index}`}
                            value={tag.value}
                            label="Timezone"
                            onChange={(e) =>
                              handleUpdateTag(index, 'value', e.target.value)
                            }
                            disabled={isSubmitting}
                            fullWidth
                          >
                            {ALL_TIMEZONES.map((tz) => (
                              <MenuItem key={tz} value={tz}>
                                {tz}
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
                            handleUpdateTag(index, 'value', e.target.value)
                          }
                          disabled={isSubmitting}
                          sx={{ flex: 1 }}
                        />
                      )}
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
