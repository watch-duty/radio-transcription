import { useEffect, useState } from 'react';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { Feed, FeedCreate, FeedUpdate } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createFeed } from '../../service/createFeed';
import { listFeeds } from '../../service/listFeeds';
import { updateFeed } from '../../service/updateFeed';
import { FeedConfigurationEdit } from './FeedConfigurationEdit';
import { FeedConfigurationTable } from './FeedConfigurationTable';

interface FeedConfigurationViewProps {
  triggerSnackbar?: (message: string) => void;
  onError?: (error: Error, titleMessage?: string) => void;
}

export function FeedConfigurationView({
  triggerSnackbar,
  onError,
}: FeedConfigurationViewProps) {
  const { token } = useAuth();
  const queryClient = useQueryClient();

  // Mode: null if creating, Feed object if updating/editing
  const [editingFeed, setEditingFeed] = useState<Feed | null>(null);

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

  // TanStack Query Mutations
  const createMutation = useMutation({
    mutationFn: (newFeed: FeedCreate) => createFeed(newFeed, token!),
    onSuccess: (data) => {
      if (triggerSnackbar) {
        triggerSnackbar(`Feed "${data.name}" registered successfully!`);
      }
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
      queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
    },
    onError: (error: Error) => {
      if (onError) {
        onError(error, 'Updating Feed Settings');
      }
    },
  });

  const handleCreateFeed = async (payload: FeedCreate) => {
    await createMutation.mutateAsync(payload);
  };

  const handleUpdateFeed = async (feedId: string, payload: FeedUpdate) => {
    await updateMutation.mutateAsync({ feedId, updatePayload: payload });
  };

  const handleStartEdit = (feed: Feed) => {
    setEditingFeed(feed);
    // Smooth scroll operator back to form on small viewports
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const handleCancelEdit = () => {
    setEditingFeed(null);
  };

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
          <FeedConfigurationEdit
            editingFeed={editingFeed}
            onCreateFeed={handleCreateFeed}
            onUpdateFeed={handleUpdateFeed}
            onCancel={handleCancelEdit}
            isSubmitting={isSubmitting}
          />
        </Grid>

        <Grid size={{ xs: 12, sm: 8 }}>
          <FeedConfigurationTable
            feeds={feeds}
            feedsLoading={feedsLoading}
            editingFeedId={editingFeed?.id}
            onEditFeed={handleStartEdit}
            isSubmitting={isSubmitting}
          />
        </Grid>
      </Grid>
    </Box>
  );
}

export default FeedConfigurationView;
