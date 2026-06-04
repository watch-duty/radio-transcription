import { useState } from 'react';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQueryClient } from '@tanstack/react-query';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { SourceType } from '@transcription/common';
import type { Feed, FeedCreate, FeedUpdate } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createFeed } from '../../service/createFeed';
import { deleteFeed } from '../../service/deleteFeed';
import { updateFeed } from '../../service/updateFeed';
import { FeedConfigurationEdit } from './FeedConfigurationEdit';
import { FeedTable } from './FeedTable';

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
  const [tags, setTags] = useState<{ key: string; value: string }[]>([]);

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
    onSuccess: () => {
      triggerSnackbar('Feed deleted successfully!');
      setIsEditing(false);
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Deleting Feed');
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
    deleteMutation.isPending;

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
            onUpdateFeed={(payload: FeedUpdate) =>
              handleUpdateFeed(id, payload)
            }
            onDeleteFeed={async () => {
              await deleteMutation.mutateAsync(id);
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
            allowEdit
            editingFeedId={isEditing ? id : undefined}
            onEditFeed={handleStartEdit}
            isSubmitting={isSubmitting}
            onError={onError}
          />
        </Grid>
      </Grid>
    </Box>
  );
}

export default FeedConfigurationView;
