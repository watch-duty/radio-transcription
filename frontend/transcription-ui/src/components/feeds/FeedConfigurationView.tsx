import { useEffect, useMemo, useRef, useState } from 'react';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import Backdrop from '@mui/material/Backdrop';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  Feed,
  FeedCreate,
  FeedUpdate,
  ListFeedsResponse,
  Tag,
} from '@transcription/common';
import { SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createFeed } from '../../service/createFeed';
import { deactivateFeed } from '../../service/deactivateFeed';
import { deleteFeed } from '../../service/deleteFeed';
import { listFeeds } from '../../service/listFeeds';
import { resetFeed } from '../../service/resetFeed';
import { updateFeed } from '../../service/updateFeed';
import { FeedConfigurationEdit } from './FeedConfigurationEdit';
import { type FeedFilters, FeedTable } from './FeedTable';
import { type TagRow, toTagRows } from './tagRows';

interface FeedConfigurationViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const QUERY_DEBOUNCE_TIME_MS = 300;

function hasTimezoneChanged(
  originalTags: Tag[] | undefined,
  newTags: Tag[] | undefined
): boolean {
  const origTz = originalTags?.find((t) => t.key === 'system/timezone')?.value;
  const newTz = newTags?.find((t) => t.key === 'system/timezone')?.value;
  return origTz !== newTz;
}

export function FeedConfigurationView({
  triggerSnackbar,
  onError,
}: FeedConfigurationViewProps) {
  const { token } = useAuth();
  const queryClient = useQueryClient();

  const [isEditing, setIsEditing] = useState(false);
  const [id, setId] = useState('');
  const [name, setName] = useState('');
  const [sourceType, setSourceType] = useState(SourceType.BCFY_FEEDS);
  const [sourceFeedId, setSourceFeedId] = useState('');
  const [tags, setTags] = useState<TagRow[]>([]);
  const [orchestrationStatus, setOrchestrationStatus] = useState<string | null>(
    null
  );

  const [filters, setFilters] = useState<FeedFilters>({
    searchQuery: '',
    sourceTypes: [],
    statuses: [],
    tags: [],
  });

  // We are only debouncing the query because the keystrokes can be fast and we want to avoid querying every keypress.
  // However for the filter selections, selecting options is a bit slower and we want to show immediate feedback from checkboxes.
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState(
    filters.searchQuery
  );

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchQuery(filters.searchQuery);
    }, QUERY_DEBOUNCE_TIME_MS);
    return () => clearTimeout(handler);
  }, [filters.searchQuery]);

  const feedsErrorHandled = useRef<Error | null>(null);

  // We flatten the filter object and include array lengths in the query key.
  // In JavaScript, empty arrays ([]) are compared by object reference rather than value.
  // Flattening the keys and including primitive lengths (e.g. 0) ensures that we can define
  // a stable, static query key for `allFeeds` (['listFeeds', token, '', [], 0, [], 0, [], 0])
  // that matches the structure of the main query, allowing `queryClient.setQueriesData`
  // and `invalidateQueries` prefix matching to successfully update both query caches at once.
  const {
    data: feedsData,
    isLoading: feedsLoading,
    error: feedsError,
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
  });

  const feeds = useMemo(() => feedsData?.feeds ?? [], [feedsData]);
  const feedTotal = feedsData?.total ?? 0;

  useEffect(() => {
    if (feedsError && feedsErrorHandled.current !== feedsError) {
      feedsErrorHandled.current = feedsError;
      if (onError) {
        onError(feedsError, 'Loading Configured Feeds');
      }
    }
  }, [feedsError, onError]);

  // TODO: https://linear.app/watchduty/issue/GOO-575 - Remove allFeeds once the tags are computed in the backend
  const { data: allFeedData = { feeds: [], total: 0 } } = useQuery({
    queryKey: ['listFeeds', token, '', [], 0, [], 0, [], 0],
    queryFn: () => listFeeds(token!, {}),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  const allFeeds = allFeedData.feeds;

  // TODO: https://linear.app/watchduty/issue/GOO-575 - Provide filter tags in backend
  const uniqueTagsForFilter = useMemo<{ key: string; value: string }[]>(() => {
    const seen = new Set<string>();
    const result: { key: string; value: string }[] = [];
    const sourceFeeds = allFeeds || [];
    sourceFeeds.forEach((feed) => {
      feed.tags?.forEach((tag) => {
        const identifier = `${tag.key}:${tag.value}`;
        if (!seen.has(identifier)) {
          seen.add(identifier);
          result.push({ key: tag.key, value: tag.value });
        }
      });
    });
    return result.sort(
      (a, b) => a.key.localeCompare(b.key) || a.value.localeCompare(b.value)
    );
  }, [allFeeds]);

  const resetForm = () => {
    setId('');
    setName('');
    setSourceType(SourceType.BCFY_FEEDS);
    setSourceFeedId('');
    setTags([]);
  };

  const resetFormAndRefresh = () => {
    resetForm();
    queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
  };

  const createMutation = useMutation({
    mutationFn: (newFeed: FeedCreate) => createFeed(newFeed, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Feed "${data.name}" registered successfully!`);
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
    },
    onError: (error: Error) => {
      onError(error, 'Registering Feed');
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
      triggerSnackbar(`Feed "${data.name}" updated successfully!`);
      setIsEditing(false);
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Updating Feed Settings');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (feedId: string) => deleteFeed(feedId, token!),
    onSuccess: (_, feedId) => {
      triggerSnackbar('Feed deleted successfully!');
      setIsEditing(false);
      queryClient.setQueriesData<ListFeedsResponse>(
        { queryKey: ['listFeeds', token] },
        (oldData) => {
          if (!oldData) return oldData;
          const updatedFeeds = oldData.feeds.filter((f) => f.id !== feedId);
          return {
            feeds: updatedFeeds,
            total: oldData.total - (oldData.feeds.length - updatedFeeds.length),
          };
        }
      );
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Deleting Feed');
    },
  });

  const deactivateMutation = useMutation({
    mutationFn: (feedId: string) => deactivateFeed(feedId, token!),
    onSuccess: () => {
      triggerSnackbar('Feed deactivated successfully!');
      setIsEditing(false);
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Deactivating Feed');
    },
  });

  const resetMutation = useMutation({
    mutationFn: (feedId: string) => resetFeed(feedId, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Feed "${data.name}" reset successfully!`);
      setIsEditing(false);
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Resetting Feed');
    },
  });

  const handleCreateFeed = async (payload: FeedCreate) => {
    await createMutation.mutateAsync(payload);
  };

  const handleUpdateFeed = async (feedId: string, payload: FeedUpdate) => {
    const originalFeed = feeds.find((f) => f.id === feedId);
    const tzChanged = hasTimezoneChanged(originalFeed?.tags, payload.tags);
    const isRunning =
      originalFeed?.substatus === 'active' ||
      originalFeed?.substatus === 'failing';

    if (tzChanged && isRunning) {
      try {
        setOrchestrationStatus('Updating feed configuration...');
        await updateFeed(feedId, payload, token!);

        setOrchestrationStatus('Stopping active ingestion...');
        await deactivateFeed(feedId, token!);

        for (let i = 20; i > 0; i--) {
          setOrchestrationStatus(
            `Waiting for worker to release lease safely (${i}s)...`
          );
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }

        setOrchestrationStatus('Restarting feed...');
        await resetFeed(feedId, token!);

        triggerSnackbar(
          `Feed "${payload.name}" updated and restarted successfully!`
        );
      } catch (error) {
        onError(error as Error, 'Orchestrated Feed Restart');
      } finally {
        setOrchestrationStatus(null);
        setIsEditing(false);
        resetFormAndRefresh();
      }
    } else {
      await updateMutation.mutateAsync({ feedId, updatePayload: payload });
    }
  };

  const handleStartEdit = (feed: Feed) => {
    setIsEditing(true);
    setId(feed.id);
    setName(feed.name);
    setSourceType(feed.sourceType);
    setSourceFeedId(feed.sourceFeedId || '');
    setTags(toTagRows(feed.tags ?? []));
    // Smooth scroll operator back to form on small viewports
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    resetForm();
  };

  const isSubmitting =
    createMutation.isPending ||
    updateMutation.isPending ||
    deleteMutation.isPending ||
    deactivateMutation.isPending ||
    resetMutation.isPending ||
    orchestrationStatus !== null;

  const currentEditingFeed = feeds.find((f) => f.id === id);

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
        gap: 2,
        overflow: { xs: 'visible', sm: 'hidden' },
      }}
    >
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'row',
          alignItems: 'center',
          gap: 1,
        }}
      >
        <AppRegistrationIcon
          sx={{
            fontSize: 32,
            color: 'primary.main',
          }}
        />
        <Typography variant="h4" sx={{ fontWeight: 600 }}>
          Feed Configuration
        </Typography>
      </Box>

      <Grid
        container
        spacing={4}
        sx={{
          flexGrow: 1,
          minHeight: 0,
        }}
      >
        <Grid
          size={{ xs: 12, sm: 4 }}
          sx={{
            display: 'flex',
            flexDirection: 'column',
            height: { xs: 'auto', sm: '100%' },
            minHeight: { xs: 'auto', sm: 0 },
          }}
        >
          <FeedConfigurationEdit
            key={isEditing ? `edit-${sourceFeedId}` : 'register'}
            isEditing={isEditing}
            feedName={name}
            feedSourceType={sourceType}
            feedSourceId={sourceFeedId}
            feedTags={tags}
            feedStatus={currentEditingFeed?.status}
            feedSubstatus={currentEditingFeed?.substatus}
            setFeedName={setName}
            setFeedSourceType={setSourceType}
            setFeedSourceId={setSourceFeedId}
            setFeedTags={setTags}
            onCreateFeed={handleCreateFeed}
            onUpdateFeed={(payload: FeedUpdate) =>
              handleUpdateFeed(id, payload)
            }
            onDeleteFeed={async () => {
              await deleteMutation.mutateAsync(id);
            }}
            onDeactivateFeed={async () => {
              await deactivateMutation.mutateAsync(id);
            }}
            onResetFeed={async () => {
              await resetMutation.mutateAsync(id);
            }}
            onCancel={handleCancelEdit}
            isSubmitting={isSubmitting}
          />
        </Grid>

        <Grid
          size={{ xs: 12, sm: 8 }}
          sx={{
            display: 'flex',
            flexDirection: 'column',
            height: { xs: 'auto', sm: '100%' },
            minHeight: { xs: 'auto', sm: 0 },
          }}
        >
          <FeedTable
            feeds={feeds}
            tags={uniqueTagsForFilter}
            isLoading={feedsLoading}
            feedTotal={feedTotal}
            allowEdit
            editingFeedId={isEditing ? id : undefined}
            onEditFeed={handleStartEdit}
            isSubmitting={isSubmitting}
            filters={filters}
            onFiltersChange={setFilters}
          />
        </Grid>
      </Grid>
      <Backdrop
        sx={{
          color: '#fff',
          zIndex: (theme) => theme.zIndex.drawer + 1,
          flexDirection: 'column',
          gap: 2,
        }}
        open={orchestrationStatus !== null}
      >
        <CircularProgress color="inherit" />
        <Typography variant="h6" component="div">
          {orchestrationStatus}
        </Typography>
      </Backdrop>
    </Box>
  );
}

export default FeedConfigurationView;
