import { forwardRef, useEffect, useMemo, useState } from 'react';
import type { ComponentProps, HTMLAttributes } from 'react';
import { TableVirtuoso } from 'react-virtuoso';

import AddIcon from '@mui/icons-material/Add';
import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import ClearIcon from '@mui/icons-material/Clear';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import RssFeedIcon from '@mui/icons-material/RssFeed';
import SearchIcon from '@mui/icons-material/Search';
import TagIcon from '@mui/icons-material/Tag';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import FormHelperText from '@mui/material/FormHelperText';
import Grid from '@mui/material/Grid';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import Paper from '@mui/material/Paper';
import Select from '@mui/material/Select';
import Stack from '@mui/material/Stack';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import TableSortLabel from '@mui/material/TableSortLabel';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  Feed,
  FeedCreate,
  FeedUpdate,
  SourceType,
  Tag,
} from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createFeed } from '../../service/createFeed';
import { listFeeds } from '../../service/listFeeds';
import { updateFeed } from '../../service/updateFeed';
import { FeedStatusIndicator } from '../common/FeedStatusIndicator';

const VirtuosoScroller = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => <TableContainer {...props} ref={ref} component="div" />);
VirtuosoScroller.displayName = 'VirtuosoScroller';

const VirtuosoTableHead = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <TableHead {...props} ref={ref} component="div" sx={{ display: 'block' }} />
));
VirtuosoTableHead.displayName = 'VirtuosoTableHead';

const VirtuosoTableBody = forwardRef<
  HTMLDivElement,
  HTMLAttributes<HTMLDivElement>
>((props, ref) => (
  <TableBody {...props} ref={ref} component="div" sx={{ display: 'block' }} />
));
VirtuosoTableBody.displayName = 'VirtuosoTableBody';

const VirtuosoTable = forwardRef<HTMLDivElement, ComponentProps<typeof Table>>(
  (props, ref) => (
    <Table
      {...props}
      ref={ref}
      component="div"
      sx={{ display: 'block', width: '100%' }}
    />
  )
);
VirtuosoTable.displayName = 'VirtuosoTable';

const GRID_TEMPLATE_COLUMNS = '1.5fr 1fr 1fr 60px';

function VirtuosoTableRow(
  props: ComponentProps<typeof TableRow> & {
    item?: Feed;
    context?: { editingFeedId?: string };
  }
) {
  const { item, context, ...rest } = props;
  const isSelected = !!(item && context?.editingFeedId === item.id);

  return (
    <TableRow
      {...rest}
      component="div"
      hover
      selected={isSelected}
      sx={{
        display: 'grid',
        gridTemplateColumns: GRID_TEMPLATE_COLUMNS,
        width: '100%',
        alignItems: 'center',
        borderBottom: '1px solid',
        borderColor: 'divider',
        borderLeft: '4px solid transparent',
        transition: 'background-color 0.2s ease, border-left-color 0.2s ease',
        ...(isSelected && {
          bgcolor: 'action.selected',
          borderLeftColor: 'warning.main',
        }),
        ...rest.sx,
      }}
    />
  );
}

const VIRTUOSO_COMPONENTS = {
  Scroller: VirtuosoScroller,
  Table: VirtuosoTable,
  TableHead: VirtuosoTableHead,
  TableRow: VirtuosoTableRow,
  TableBody: VirtuosoTableBody,
};

interface FeedConfigurationViewProps {
  triggerSnackbar?: (message: string) => void;
  onError?: (error: Error, titleMessage?: string) => void;
}

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

export function FeedConfigurationView({
  triggerSnackbar,
  onError,
}: FeedConfigurationViewProps) {
  const theme = useTheme();
  const { token } = useAuth();
  const queryClient = useQueryClient();

  // Mode: null if creating, Feed object if updating/editing
  const [editingFeed, setEditingFeed] = useState<Feed | null>(null);

  // Form Fields
  const [name, setName] = useState('');
  const [sourceType, setSourceType] = useState<SourceType>('bcfy_feeds');
  const [sourceFeedId, setSourceFeedId] = useState('');
  const [externalId, setExternalId] = useState('');
  const [tags, setTags] = useState<Tag[]>([]);

  // Subform dynamic fields for Tags
  const [newTagKey, setNewTagKey] = useState('');
  const [newTagValue, setNewTagValue] = useState('');

  // Local validation error states
  const [validationErrors, setValidationErrors] = useState<
    Record<string, string>
  >({});

  // Search filter query for existing feeds column
  const [feedSearchQuery, setFeedSearchQuery] = useState('');

  // Table sorting states (Default sorting is Feed Name ascending)
  const [sortBy, setSortBy] = useState<'name' | 'type' | 'status'>('name');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  const handleRequestSort = (property: 'name' | 'type' | 'status') => {
    const isAsc = sortBy === property && sortDirection === 'asc';
    setSortDirection(isAsc ? 'desc' : 'asc');
    setSortBy(property);
  };

  const resetFormState = () => {
    setName('');
    setSourceType('bcfy_feeds');
    setSourceFeedId('');
    setExternalId('');
    setTags([]);
    setNewTagKey('');
    setNewTagValue('');
    setValidationErrors({});
  };

  // Query existing feeds to show on the right panel list
  const {
    data: feeds = [],
    isLoading: feedsLoading,
    error: feedsError,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  // Handle feed query errors in side effects
  useEffect(() => {
    if (feedsError && onError) {
      onError(feedsError, 'Loading Configured Feeds');
    }
  }, [feedsError, onError]);

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
      errors.name = 'Feed display name is required.';
    }

    // Source Feed ID and Source Type are only required when creating.
    // In update mode they are permanent, but we validate to stay completely defensive.
    if (!editingFeed) {
      if (!sourceFeedId.trim()) {
        errors.sourceFeedId = 'Source feed ID is required.';
      }
    }

    if (!externalId.trim()) {
      errors.externalId = 'External ID is required.';
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

  // TanStack Query Mutations
  const createMutation = useMutation({
    mutationFn: (newFeed: FeedCreate) => createFeed(newFeed, token!),
    onSuccess: (data) => {
      if (triggerSnackbar) {
        triggerSnackbar(`Feed "${data.name}" registered successfully!`);
      }
      resetFormState();
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
    },
    onError: (error: Error) => {
      if (onError) {
        onError(error, 'Registering Feed');
      }
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({
      feedId,
      updatePayload,
    }: {
      feedId: string;
      updatePayload: FeedUpdate;
    }) => updateFeed(feedId, updatePayload, token!),
    onSuccess: (data) => {
      if (triggerSnackbar) {
        triggerSnackbar(`Feed "${data.name}" updated successfully!`);
      }
      setEditingFeed(null);
      resetFormState();
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
    },
    onError: (error: Error) => {
      if (onError) {
        onError(error, 'Updating Feed Settings');
      }
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    if (editingFeed) {
      const payload: FeedUpdate = {
        name: name.trim(),
        externalId: externalId.trim(),
        tags: tags.length > 0 ? tags : undefined,
      };
      updateMutation.mutate({
        feedId: editingFeed.id,
        updatePayload: payload,
      });
    } else {
      const payload: FeedCreate = {
        name: name.trim(),
        sourceType,
        sourceFeedId: sourceFeedId.trim(),
        externalId: externalId.trim(),
        tags: tags.length > 0 ? tags : undefined,
      };
      createMutation.mutate(payload);
    }
  };

  const handleStartEdit = (feed: Feed) => {
    setEditingFeed(feed);
    setName(feed.name);
    setSourceType(feed.sourceType);
    setSourceFeedId(feed.sourceFeedId || '');
    setExternalId(feed.externalId || '');
    setTags(feed.tags || []);

    setValidationErrors({});
    // Smooth scroll operator back to form on small viewports
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const handleCancelEdit = () => {
    setEditingFeed(null);
    resetFormState();
  };

  // Filter list of existing feeds dynamically
  const filteredFeeds = useMemo(() => {
    const query = feedSearchQuery.toLowerCase().trim();
    const result = query
      ? feeds.filter((feed) => {
          const nameMatch = feed.name.toLowerCase().includes(query);
          const extMatch =
            feed.externalId?.toLowerCase().includes(query) || false;
          const tagMatch =
            feed.tags?.some(
              (t) =>
                t.key.toLowerCase().includes(query) ||
                t.value.toLowerCase().includes(query)
            ) ?? false;
          return nameMatch || extMatch || tagMatch;
        })
      : [...feeds];

    result.sort((a, b) => {
      let valA = '';
      let valB = '';

      if (sortBy === 'name') {
        valA = a.name.toLowerCase();
        valB = b.name.toLowerCase();
      } else if (sortBy === 'type') {
        valA = a.sourceType.toLowerCase();
        valB = b.sourceType.toLowerCase();
      } else if (sortBy === 'status') {
        valA = a.status.toLowerCase();
        valB = b.status.toLowerCase();
      }

      if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
      if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
      return 0;
    });

    return result;
  }, [feeds, feedSearchQuery, sortBy, sortDirection]);

  const isSubmitting = createMutation.isPending || updateMutation.isPending;

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        gap: 3,
        py: 1,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
        <AppRegistrationIcon
          sx={{
            fontSize: 32,
            color: editingFeed ? 'warning.main' : 'primary.main',
          }}
        />
        <Typography variant="h4" sx={{ fontWeight: 600 }}>
          Feed Configuration
        </Typography>
      </Box>

      <Grid container spacing={4} sx={{ width: '100%', m: 0 }}>
        <Grid size={{ xs: 12, sm: 4 }}>
          <Card
            elevation={0}
            variant="outlined"
            data-testid="feed-config-card"
            sx={{
              borderRadius: 3,
              overflow: 'hidden',
              boxShadow: '0 4px 20px rgba(0, 0, 0, 0.04)',
              border: 1,
              borderColor: 'divider',
              display: 'flex',
              flexDirection: 'column',
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
              sx={{ p: 3, display: 'flex', flexDirection: 'column', gap: 3 }}
            >
              <Box component="form" onSubmit={handleSubmit} noValidate>
                <Stack spacing={3}>
                  <TextField
                    fullWidth
                    label="Feed Display Name"
                    variant="outlined"
                    placeholder="e.g. Ventura Public Safety - Fire Dispatch"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    error={!!validationErrors.name}
                    helperText={
                      validationErrors.name || 'Display name of the feed.'
                    }
                    disabled={isSubmitting}
                  />

                  <Grid container spacing={2}>
                    <Grid size={{ xs: 12, sm: 6 }}>
                      <FormControl
                        fullWidth
                        disabled={!!editingFeed || isSubmitting}
                      >
                        <InputLabel id="source-type-select-label">
                          Source Type
                        </InputLabel>
                        <Select
                          labelId="source-type-select-label"
                          id="source-type-select"
                          value={sourceType}
                          label="Source Type"
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
                            ? 'Source cannot be changed after it has been registered.'
                            : 'Source of the feed audio.'}
                        </FormHelperText>
                      </FormControl>
                    </Grid>

                    <Grid size={{ xs: 12, sm: 6 }}>
                      <TextField
                        fullWidth
                        label="Source Feed ID"
                        variant="outlined"
                        placeholder={
                          sourceType === 'bcfy_feeds'
                            ? 'e.g. 12345'
                            : 'e.g. system-slug'
                        }
                        value={sourceFeedId}
                        onChange={(e) => setSourceFeedId(e.target.value)}
                        error={!!validationErrors.sourceFeedId}
                        helperText={validationErrors.sourceFeedId}
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

                  <TextField
                    fullWidth
                    label="External ID"
                    variant="outlined"
                    placeholder="e.g. ca-mrn-fire-10"
                    value={externalId}
                    onChange={(e) => setExternalId(e.target.value)}
                    error={!!validationErrors.externalId}
                    helperText={
                      validationErrors.externalId ||
                      'Internal identifier tag for systems lookup, e.g. "ca-mrn-fd-1".'
                    }
                    disabled={isSubmitting}
                  />

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
                        placeholder="e.g. county"
                        value={newTagKey}
                        onChange={(e) => setNewTagKey(e.target.value)}
                        error={!!validationErrors.tags}
                        disabled={isSubmitting}
                        sx={{ flexGrow: 1 }}
                      />
                      <TextField
                        size="small"
                        label="Tag Value"
                        placeholder="e.g. Ventura"
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
                        onClick={handleCancelEdit}
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
        </Grid>

        <Grid size={{ xs: 12, sm: 8 }}>
          <Paper
            data-testid="feeds-deck-card"
            variant="outlined"
            sx={{
              p: 3,
              borderRadius: 3,
              display: 'flex',
              flexDirection: 'column',
              minHeight: 500,
              height: 'calc(100vh - 220px)',
              overflow: 'hidden',
              boxShadow: '0 4px 20px rgba(0, 0, 0, 0.03)',
            }}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                mb: 2,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <RssFeedIcon color="primary" fontSize="small" />
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Registered feeds
                </Typography>
              </Box>
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ fontWeight: 500 }}
              >
                {feeds.length} Feeds
              </Typography>
            </Box>

            {/* Quick Filter Input Bar */}
            <TextField
              fullWidth
              size="small"
              placeholder="Filter feeds by name, tags, or ID..."
              value={feedSearchQuery}
              onChange={(e) => setFeedSearchQuery(e.target.value)}
              slotProps={{
                input: {
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" color="action" />
                    </InputAdornment>
                  ),
                  endAdornment: feedSearchQuery ? (
                    <InputAdornment position="end">
                      <IconButton
                        size="small"
                        onClick={() => setFeedSearchQuery('')}
                      >
                        <ClearIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  ) : null,
                },
              }}
              sx={{ mb: 2.5 }}
            />

            <Divider />

            {feedsLoading ? (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center',
                  flexGrow: 1,
                  py: 6,
                }}
              >
                <Stack spacing={2} sx={{ alignItems: 'center' }}>
                  <CircularProgress size={36} thickness={4} />
                  <Typography variant="body2" color="text.secondary">
                    Loading registered feeds...
                  </Typography>
                </Stack>
              </Box>
            ) : filteredFeeds.length === 0 ? (
              <Box
                sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flexGrow: 1,
                  py: 6,
                  textAlign: 'center',
                  px: 3,
                }}
              >
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ fontWeight: 500 }}
                >
                  {feedSearchQuery
                    ? 'No feeds matching filter query found.'
                    : 'No feed found.'}
                </Typography>
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ mt: 0.5 }}
                >
                  {feedSearchQuery
                    ? 'Refine spelling or delete terms to widen search scope.'
                    : 'Register feeds on the left to start listening.'}
                </Typography>
              </Box>
            ) : (
              <TableVirtuoso
                style={{ flexGrow: 1, marginTop: 12 }}
                data={filteredFeeds}
                context={{ editingFeedId: editingFeed?.id }}
                computeItemKey={(_index, feed) => feed.id}
                components={VIRTUOSO_COMPONENTS}
                fixedHeaderContent={() => (
                  <TableRow
                    component="div"
                    sx={{
                      display: 'grid',
                      gridTemplateColumns: GRID_TEMPLATE_COLUMNS,
                      width: '100%',
                      bgcolor: 'background.paper',
                    }}
                  >
                    <TableCell
                      component="div"
                      sx={{
                        fontWeight: 'bold',
                        bgcolor: 'background.paper',
                      }}
                      sortDirection={sortBy === 'name' ? sortDirection : false}
                    >
                      <TableSortLabel
                        active={sortBy === 'name'}
                        direction={sortBy === 'name' ? sortDirection : 'asc'}
                        onClick={() => handleRequestSort('name')}
                      >
                        Name
                      </TableSortLabel>
                    </TableCell>
                    <TableCell
                      component="div"
                      sx={{
                        fontWeight: 'bold',
                        bgcolor: 'background.paper',
                      }}
                      sortDirection={sortBy === 'type' ? sortDirection : false}
                    >
                      <TableSortLabel
                        active={sortBy === 'type'}
                        direction={sortBy === 'type' ? sortDirection : 'asc'}
                        onClick={() => handleRequestSort('type')}
                      >
                        Type
                      </TableSortLabel>
                    </TableCell>
                    <TableCell
                      component="div"
                      sx={{
                        fontWeight: 'bold',
                        bgcolor: 'background.paper',
                      }}
                      sortDirection={
                        sortBy === 'status' ? sortDirection : false
                      }
                    >
                      <TableSortLabel
                        active={sortBy === 'status'}
                        direction={sortBy === 'status' ? sortDirection : 'asc'}
                        onClick={() => handleRequestSort('status')}
                      >
                        Status
                      </TableSortLabel>
                    </TableCell>

                    <TableCell
                      component="div"
                      align="right"
                      sx={{
                        fontWeight: 'bold',
                        bgcolor: 'background.paper',
                      }}
                    />
                  </TableRow>
                )}
                itemContent={(_index, feed) => {
                  const isCurrentlyEditingThis = editingFeed?.id === feed.id;

                  return (
                    <>
                      {/* Name & ID Metadata */}
                      <TableCell
                        component="div"
                        sx={{
                          py: 1,
                          display: 'flex',
                          flexDirection: 'column',
                          borderBottom: 'none',
                          minWidth: 0,
                        }}
                      >
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {feed.name}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          <b>Source ID:</b> {feed.sourceFeedId}
                        </Typography>
                        {feed.externalId && (
                          <Typography variant="caption" color="text.secondary">
                            <b>External ID:</b> {feed.externalId}
                          </Typography>
                        )}
                      </TableCell>

                      {/* Source Type Chip */}
                      <TableCell
                        component="div"
                        sx={{ borderBottom: 'none', minWidth: 0 }}
                      >
                        <Chip
                          label={feed.sourceType}
                          size="small"
                          variant="outlined"
                        />
                      </TableCell>

                      {/* Status Indicator */}
                      <TableCell
                        component="div"
                        sx={{ borderBottom: 'none', minWidth: 0 }}
                      >
                        <FeedStatusIndicator
                          status={feed.status}
                          lastHeartbeat={feed.lastHeartbeat}
                        />
                      </TableCell>

                      {/* Actions Buttons */}
                      <TableCell
                        align="right"
                        component="div"
                        sx={{ borderBottom: 'none' }}
                      >
                        <IconButton
                          size="small"
                          onClick={() => handleStartEdit(feed)}
                          disabled={isSubmitting || isCurrentlyEditingThis}
                          sx={{
                            border: '1px solid',
                            borderRadius: 1.5,
                            p: 0.5,
                            '&:hover': {
                              borderColor: 'primary.main',
                              bgcolor: 'primary.soft',
                              color: 'primary.main',
                            },
                          }}
                          aria-label={`Edit ${feed.name}`}
                        >
                          <EditIcon fontSize="small" />
                        </IconButton>
                      </TableCell>

                      {feed.tags && feed.tags.length > 0 && (
                        <TableCell
                          component="div"
                          sx={{
                            gridColumn: '1 / -1',
                            borderBottom: 'none',
                            pt: 0,
                            display: 'flex',
                            flexWrap: 'wrap',
                            gap: 0.75,
                          }}
                        >
                          {feed.tags.map((tag, i) => (
                            <Chip
                              key={i}
                              label={
                                <Box>
                                  <b>{tag.key}</b>: {tag.value}
                                </Box>
                              }
                              size="small"
                            />
                          ))}
                        </TableCell>
                      )}
                    </>
                  );
                }}
              />
            )}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
}

export default FeedConfigurationView;
