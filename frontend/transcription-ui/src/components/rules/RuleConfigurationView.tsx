import { useEffect, useMemo, useRef, useState } from 'react';

import RuleIcon from '@mui/icons-material/Rule';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import type { Rule, RuleCreate, RuleUpdate } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { createRule } from '../../service/createRule';
import { deleteRule } from '../../service/deleteRule';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { updateRule } from '../../service/updateRule';
import { RuleConfigurationEdit } from './RuleConfigurationEdit';
import { RuleTable } from './RuleTable';

interface RuleConfigurationViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const QUERY_DEBOUNCE_TIME_MS = 300;

export function RuleConfigurationView({
  triggerSnackbar,
  onError,
}: RuleConfigurationViewProps) {
  const { token, isAdmin } = useAuth();
  const queryClient = useQueryClient();

  const [isEditing, setIsEditing] = useState(false);
  const [id, setId] = useState('');
  const [editingRule, setEditingRule] = useState<RuleCreate>(() => ({
    ruleName: '',
    description: '',
    isActive: true,
    scope: { level: 'GLOBAL', targetFeeds: [] },
    conditions: {
      evaluationType: 'KEYWORD_MATCH',
      operator: 'ANY',
      keywords: [],
      caseSensitive: false,
    },
  }));

  const [feedSearchQuery, setFeedSearchQuery] = useState('');
  const [debouncedFeedSearchQuery, setDebouncedFeedSearchQuery] = useState('');

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedFeedSearchQuery(feedSearchQuery);
    }, QUERY_DEBOUNCE_TIME_MS);
    return () => clearTimeout(handler);
  }, [feedSearchQuery]);

  const rulesErrorHandled = useRef<Error | null>(null);
  const feedsErrorHandled = useRef<Error | null>(null);

  const {
    data: rules = [],
    isLoading: rulesLoading,
    error: rulesError,
  } = useQuery({
    queryKey: ['listRules', token],
    queryFn: () => listRules(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  const {
    data: feedsData,
    isLoading: feedsLoading,
    error: feedsError,
    hasNextPage: hasNextFeedsPage,
    isFetchingNextPage: isFetchingNextFeedsPage,
    fetchNextPage: fetchNextFeedsPage,
  } = useInfiniteQuery({
    queryKey: ['listFeeds', token, debouncedFeedSearchQuery],
    queryFn: ({ pageParam }) =>
      listFeeds(token!, {
        limit: 50,
        nextToken: pageParam || undefined,
        name: debouncedFeedSearchQuery || undefined,
      }),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.nextToken || undefined,
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  const feeds = useMemo(
    () => feedsData?.pages.flatMap((page) => page.feeds) ?? [],
    [feedsData]
  );

  const sortedFeeds = useMemo(() => {
    return [...feeds].sort((a, b) => a.name.localeCompare(b.name));
  }, [feeds]);

  useEffect(() => {
    if (rulesError && rulesErrorHandled.current !== rulesError) {
      rulesErrorHandled.current = rulesError;
      if (onError) {
        onError(rulesError, 'Loading Configured Rules');
      }
    }
  }, [rulesError, onError]);

  useEffect(() => {
    if (feedsError && feedsErrorHandled.current !== feedsError) {
      feedsErrorHandled.current = feedsError;
      if (onError) {
        onError(feedsError, 'Loading Feeds');
      }
    }
  }, [feedsError, onError]);

  const createMutation = useMutation({
    mutationFn: (newRule: RuleCreate) => createRule(newRule, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Rule "${data.ruleName}" created successfully!`);
      setEditingRule({
        ruleName: '',
        description: '',
        isActive: true,
        scope: { level: 'GLOBAL', targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH',
          operator: 'ANY',
          keywords: [],
          caseSensitive: false,
        },
      });
      queryClient.invalidateQueries({ queryKey: ['listRules', token] });
    },
    onError: (error: Error) => {
      onError(error, 'Creating Rule');
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({
      ruleId,
      updatePayload,
    }: {
      ruleId: string;
      updatePayload: RuleUpdate;
    }) => updateRule(ruleId, updatePayload, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Rule "${data.ruleName}" updated successfully!`);
      setIsEditing(false);
      setEditingRule({
        ruleName: '',
        description: '',
        isActive: true,
        scope: { level: 'GLOBAL', targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH',
          operator: 'ANY',
          keywords: [],
          caseSensitive: false,
        },
      });
      queryClient.invalidateQueries({ queryKey: ['listRules', token] });
    },
    onError: (error: Error) => {
      onError(error, 'Updating Rule');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (ruleId: string) => deleteRule(ruleId, token!),
    onSuccess: () => {
      triggerSnackbar('Rule deleted successfully!');
      setIsEditing(false);
      setEditingRule({
        ruleName: '',
        description: '',
        isActive: true,
        scope: { level: 'GLOBAL', targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH',
          operator: 'ANY',
          keywords: [],
          caseSensitive: false,
        },
      });
      queryClient.invalidateQueries({ queryKey: ['listRules', token] });
    },
    onError: (error: Error) => {
      onError(error, 'Deleting Rule');
    },
  });

  const handleCreateRule = (payload: RuleCreate) => {
    createMutation.mutate(payload);
  };

  const handleUpdateRule = (ruleId: string, payload: RuleUpdate) => {
    updateMutation.mutate({ ruleId, updatePayload: payload });
  };

  const handleStartEdit = (rule: Rule) => {
    setIsEditing(true);
    setId(rule.ruleId);
    setEditingRule({
      ruleName: rule.ruleName,
      description: rule.description ?? '',
      isActive: rule.isActive,
      scope: rule.scope,
      conditions: rule.conditions,
      tags: rule.tags ?? [],
    });
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setId('');
    setEditingRule({
      ruleName: '',
      description: '',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: [],
        caseSensitive: false,
      },
    });
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
        <RuleIcon
          sx={{
            fontSize: 32,
            color: 'primary.main',
          }}
        />
        <Typography variant="h4" sx={{ fontWeight: 600 }}>
          Rules
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
        {isAdmin && (
          <Grid
            size={{ xs: 12, sm: 4 }}
            sx={{
              display: 'flex',
              flexDirection: 'column',
              height: { xs: 'auto', sm: '100%' },
              minHeight: { xs: 'auto', sm: 0 },
            }}
          >
            <RuleConfigurationEdit
              key={isEditing ? `edit-${id}` : 'register'}
              isEditing={isEditing}
              editingRule={editingRule}
              setEditingRule={setEditingRule}
              editingRuleId={isEditing ? id : undefined}
              feeds={sortedFeeds}
              rules={rules}
              onCreateRule={handleCreateRule}
              onUpdateRule={(payload: RuleUpdate) =>
                handleUpdateRule(id, payload)
              }
              onDeleteRule={() => {
                deleteMutation.mutate(id);
              }}
              onCancel={handleCancelEdit}
              isSubmitting={isSubmitting}
              feedSearchQuery={feedSearchQuery}
              onFeedSearchQueryChange={setFeedSearchQuery}
              hasNextFeedsPage={hasNextFeedsPage}
              isFetchingNextFeedsPage={isFetchingNextFeedsPage}
              onFetchNextFeedsPage={fetchNextFeedsPage}
            />
          </Grid>
        )}

        <Grid
          size={{ xs: 12, sm: isAdmin ? 8 : 12 }}
          sx={{
            display: 'flex',
            flexDirection: 'column',
            height: { xs: 'auto', sm: '100%' },
            minHeight: { xs: 'auto', sm: 0 },
          }}
        >
          <RuleTable
            rules={rules}
            feeds={feeds}
            isLoading={rulesLoading || feedsLoading}
            allowEdit={isAdmin}
            editingRuleId={isEditing ? id : undefined}
            onEditRule={handleStartEdit}
            isSubmitting={isSubmitting}
          />
        </Grid>
      </Grid>
    </Box>
  );
}

export default RuleConfigurationView;
