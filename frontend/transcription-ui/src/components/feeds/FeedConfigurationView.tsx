import { useEffect, useMemo, useRef, useState } from 'react';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { InfiniteData } from '@tanstack/react-query';
import type {
  Feed,
  FeedCreate,
  FeedUpdate,
  ListFeedsResponse,
  Tag,
} from '@transcription/common';
import { SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useListFeeds } from '../../hooks/useListFeeds';
import { createFeed } from '../../service/createFeed';
import { deactivateFeed } from '../../service/deactivateFeed';
import { deleteFeed } from '../../service/deleteFeed';
import { listFeeds } from '../../service/listFeeds';
import { resetFeed } from '../../service/resetFeed';
import { updateFeed } from '../../service/updateFeed';
import { FeedConfigurationEdit } from './FeedConfigurationEdit';
import { type FeedFilters, FeedTable } from './FeedTable';

interface FeedConfigurationViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const updateFeedInCache = (
  oldData: Feed[] | InfiniteData<ListFeedsResponse> | undefined,
  updatedFeed: Feed
) => {
  if (!oldData) return oldData;
  if ('pages' in oldData) {
    return {
      ...oldData,
      pages: oldData.pages.map((page) => ({
        ...page,
        feeds: page.feeds.map((f) => (f.id === updatedFeed.id ? updatedFeed : f)),
      })),
    };
  }
  if (Array.isArray(oldData)) {
    return oldData.map((f) => (f.id === updatedFeed.id ? updatedFeed : f));
  }
  return oldData;
};

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
  const [tags, setTags] = useState<Tag[]>([]);

  const [filters, setFilters] = useState<FeedFilters>({
    searchQuery: '',
    sourceTypes: [],
    statuses: [],
    tags: [],
  });

  const feedsErrorHandled = useRef<Error | null>(null);

  const {
    feeds,
    isLoading: feedsLoading,
    error: feedsError,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useListFeeds({
    token,
    filters,
  });

  const { data: allFeeds = [] } = useQuery({
    queryKey: ['listFeeds', token, 'all', '', [], 0, [], 0, [], 0],
    queryFn: () => listFeeds(token!, {}),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (feedsError && feedsErrorHandled.current !== feedsError) {
      feedsErrorHandled.current = feedsError;
      if (onError) {
        onError(feedsError, 'Loading Configured Feeds');
      }
    }
  }, [feedsError, onError]);

  const resetForm = () => {
    setId('');
    setName('');
    setSourceType(SourceType.BCFY_FEEDS);
    setSourceFeedId('');
    setTags([]);
  };

  const createMutation = useMutation({
    mutationFn: (newFeed: FeedCreate) => createFeed(newFeed, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Feed "${data.name}" registered successfully!`);
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
      resetForm();
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
      queryClient.setQueriesData<Feed[] | InfiniteData<ListFeedsResponse>>(
        { queryKey: ['listFeeds', token] },
        (oldData) => updateFeedInCache(oldData, data)
      );
      resetForm();
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
      queryClient.setQueriesData<Feed[] | InfiniteData<ListFeedsResponse>>(
        { queryKey: ['listFeeds', token] },
        (oldData) => {
          if (!oldData) return oldData;
          if ('pages' in oldData) {
            return {
              ...oldData,
              pages: oldData.pages.map((page) => ({
                ...page,
                feeds: page.feeds.filter((f) => f.id !== feedId),
              })),
            };
          }
          if (Array.isArray(oldData)) {
            return oldData.filter((f) => f.id !== feedId);
          }
          return oldData;
        }
      );
      resetForm();
    },
    onError: (error: Error) => {
      onError(error, 'Deleting Feed');
    },
  });

  const deactivateMutation = useMutation({
    mutationFn: (feedId: string) => deactivateFeed(feedId, token!),
    onSuccess: (_, feedId) => {
      triggerSnackbar('Feed deactivated successfully!');
      setIsEditing(false);
      queryClient.setQueriesData<Feed[] | InfiniteData<ListFeedsResponse>>(
        { queryKey: ['listFeeds', token] },
        (oldData) => {
          if (!oldData) return oldData;
          const updateStatus = (f: Feed): Feed =>
            f.id === feedId
              ? { ...f, status: 'inactive', substatus: 'deactivated' }
              : f;
          if ('pages' in oldData) {
            return {
              ...oldData,
              pages: oldData.pages.map((page) => ({
                ...page,
                feeds: page.feeds.map(updateStatus),
              })),
            };
          }
          if (Array.isArray(oldData)) {
            return oldData.map(updateStatus);
          }
          return oldData;
        }
      );
      resetForm();
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
      queryClient.setQueriesData<Feed[] | InfiniteData<ListFeedsResponse>>(
        { queryKey: ['listFeeds', token] },
        (oldData) => updateFeedInCache(oldData, data)
      );
      resetForm();
    },
    onError: (error: Error) => {
      onError(error, 'Resetting Feed');
    },
  });

  const handleCreateFeed = async (payload: FeedCreate) => {
    await createMutation.mutateAsync(payload);
  };

  const handleUpdateFeed = async (feedId: string, payload: FeedUpdate) => {
    await updateMutation.mutateAsync({ feedId, updatePayload: payload });
  };

  const handleStartEdit = (feed: Feed) => {
    setIsEditing(true);
    setId(feed.id);
    setName(feed.name);
    setSourceType(feed.sourceType);
    setSourceFeedId(feed.sourceFeedId || '');
    setTags(feed.tags ?? []);
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
    resetMutation.isPending;

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
            allFeeds={allFeeds}
            isLoading={feedsLoading}
            allowEdit
            editingFeedId={isEditing ? id : undefined}
            onEditFeed={handleStartEdit}
            isSubmitting={isSubmitting}
            filters={filters}
            onFiltersChange={setFilters}
            hasNextPage={hasNextPage}
            onLoadMore={fetchNextPage}
            isFetchingNextPage={isFetchingNextPage}
          />
        </Grid>
      </Grid>
    </Box>
  );
}

export default FeedConfigurationView;
