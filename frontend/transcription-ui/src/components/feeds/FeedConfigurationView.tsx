import { useEffect, useRef, useState } from 'react';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { Feed, FeedCreate, FeedUpdate, Tag } from '@transcription/common';
import { SourceType } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createFeed } from '../../service/createFeed';
import { listFeeds } from '../../service/listFeeds';
import { updateFeed } from '../../service/updateFeed';
import { FeedConfigurationEdit } from './FeedConfigurationEdit';
import { FeedConfigurationTable } from './FeedConfigurationTable';

interface FeedConfigurationViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
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
  const [tags, setTags] = useState<Tag[]>([]);

  const feedsErrorHandled = useRef<Error | null>(null);

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

  useEffect(() => {
    if (feedsError && feedsErrorHandled.current !== feedsError) {
      feedsErrorHandled.current = feedsError;
      if (onError) {
        onError(feedsError, 'Loading Configured Feeds');
      }
    }
  }, [feedsError, onError]);
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
      // Reset form
      setId('');
      setName('');
      setSourceType(SourceType.BCFY_FEEDS);
      setSourceFeedId('');
      setTags([]);
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
    },
    onError: (error: Error) => {
      onError(error, 'Updating Feed Settings');
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
    // Reset form
    setId('');
    setName('');
    setSourceType(SourceType.BCFY_FEEDS);
    setSourceFeedId('');
    setTags([]);
  };

  const isSubmitting = createMutation.isPending || updateMutation.isPending;

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
            setFeedName={setName}
            setFeedSourceType={setSourceType}
            setFeedSourceId={setSourceFeedId}
            setFeedTags={setTags}
            onCreateFeed={handleCreateFeed}
            onUpdateFeed={(payload: FeedUpdate) => handleUpdateFeed(id, payload)}
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
          <FeedConfigurationTable
            feeds={feeds}
            feedsLoading={feedsLoading}
            editingFeedId={isEditing ? id : undefined}
            onEditFeed={handleStartEdit}
            isSubmitting={isSubmitting}
          />
        </Grid>
      </Grid>
    </Box>
  );
}

export default FeedConfigurationView;
