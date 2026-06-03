import { useEffect, useMemo, useRef, useState } from 'react';

import RuleIcon from '@mui/icons-material/Rule';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  EvaluationType,
  LogicalOperator,
  Rule,
  RuleCreate,
  RuleUpdate,
  ScopeLevel,
} from '@transcription/common';

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

export function RuleConfigurationView({
  triggerSnackbar,
  onError,
}: RuleConfigurationViewProps) {
  const { token } = useAuth();
  const queryClient = useQueryClient();

  const [isEditing, setIsEditing] = useState(false);
  const [id, setId] = useState('');
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [isActive, setIsActive] = useState(true);
  const [scopeLevel, setScopeLevel] = useState<ScopeLevel>('GLOBAL');
  const [targetFeeds, setTargetFeeds] = useState<string[]>([]);
  const [evaluationType, setEvaluationType] = useState<EvaluationType>('KEYWORD_MATCH');
  const [keywordOperator, setKeywordOperator] = useState<LogicalOperator>('ANY');
  const [keywords, setKeywords] = useState<string[]>([]);
  const [keywordCaseSensitive, setKeywordCaseSensitive] = useState(false);
  const [regexExpression, setRegexExpression] = useState('');
  const [regexFlags, setRegexFlags] = useState('i');
  const [groupOperator, setGroupOperator] = useState<LogicalOperator>('ANY');
  const [groupChildRuleIds, setGroupChildRuleIds] = useState<string[]>([]);

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
    data: feeds = [],
    isLoading: feedsLoading,
    error: feedsError,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

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

  const resetForm = () => {
    setId('');
    setName('');
    setDescription('');
    setIsActive(true);
    setScopeLevel('GLOBAL');
    setTargetFeeds([]);
    setEvaluationType('KEYWORD_MATCH');
    setKeywordOperator('ANY');
    setKeywords([]);
    setKeywordCaseSensitive(false);
    setRegexExpression('');
    setRegexFlags('i');
    setGroupOperator('ANY');
    setGroupChildRuleIds([]);
  };

  const resetFormAndRefresh = () => {
    resetForm();
    queryClient.invalidateQueries({ queryKey: ['listRules', token] });
  };

  const createMutation = useMutation({
    mutationFn: (newRule: RuleCreate) => createRule(newRule, token!),
    onSuccess: (data) => {
      triggerSnackbar(`Rule "${data.ruleName}" created successfully!`);
      queryClient.invalidateQueries({ queryKey: ['listRules', token] });
      resetForm();
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
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Updating Rule Settings');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (ruleId: string) => deleteRule(ruleId, token!),
    onSuccess: (_, ruleId) => {
      triggerSnackbar('Rule deleted successfully!');
      setIsEditing(false);
      queryClient.setQueryData<Rule[]>(['listRules', token], (oldRules) => {
        return oldRules ? oldRules.filter((rule) => rule.ruleId !== ruleId) : [];
      });
      resetFormAndRefresh();
    },
    onError: (error: Error) => {
      onError(error, 'Deleting Rule');
    },
  });

  const handleCreateRule = async (payload: RuleCreate) => {
    await createMutation.mutateAsync(payload);
  };

  const handleUpdateRule = async (ruleId: string, payload: RuleUpdate) => {
    await updateMutation.mutateAsync({ ruleId, updatePayload: payload });
  };

  const handleStartEdit = (rule: Rule) => {
    setIsEditing(true);
    setId(rule.ruleId);
    setName(rule.ruleName);
    setDescription(rule.description || '');
    setIsActive(rule.isActive);
    setScopeLevel(rule.scope.level);
    setTargetFeeds(rule.scope.targetFeeds);

    setEvaluationType(rule.conditions.evaluationType);

    if (rule.conditions.evaluationType === 'KEYWORD_MATCH') {
      setKeywordOperator(rule.conditions.operator);
      setKeywords(rule.conditions.keywords);
      setKeywordCaseSensitive(rule.conditions.caseSensitive);
    } else {
      setKeywordOperator('ANY');
      setKeywords([]);
      setKeywordCaseSensitive(false);
    }

    if (rule.conditions.evaluationType === 'REGEX_MATCH') {
      setRegexExpression(rule.conditions.expression);
      setRegexFlags(rule.conditions.flags);
    } else {
      setRegexExpression('');
      setRegexFlags('i');
    }

    if (rule.conditions.evaluationType === 'RULE_GROUP') {
      setGroupOperator(rule.conditions.operator);
      setGroupChildRuleIds(rule.conditions.childRuleIds);
    } else {
      setGroupOperator('ANY');
      setGroupChildRuleIds([]);
    }

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
        <RuleIcon
          sx={{
            fontSize: 32,
            color: 'primary.main',
          }}
        />
        <Typography variant="h4" sx={{ fontWeight: 600 }}>
          Rule Configuration
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
          <RuleConfigurationEdit
            key={isEditing ? `edit-${id}` : 'register'}
            isEditing={isEditing}
            ruleName={name}
            ruleDescription={description}
            ruleIsActive={isActive}
            ruleScopeLevel={scopeLevel}
            ruleTargetFeeds={targetFeeds}
            ruleEvaluationType={evaluationType}
            ruleKeywordOperator={keywordOperator}
            ruleKeywords={keywords}
            ruleKeywordCaseSensitive={keywordCaseSensitive}
            ruleRegexExpression={regexExpression}
            ruleRegexFlags={regexFlags}
            ruleGroupOperator={groupOperator}
            ruleGroupChildRuleIds={groupChildRuleIds}
            setRuleName={setName}
            setRuleDescription={setDescription}
            setRuleIsActive={setIsActive}
            setRuleScopeLevel={setScopeLevel}
            setRuleTargetFeeds={setTargetFeeds}
            setRuleEvaluationType={setEvaluationType}
            setRuleKeywordOperator={setKeywordOperator}
            setRuleKeywords={setKeywords}
            setRuleKeywordCaseSensitive={setKeywordCaseSensitive}
            setRuleRegexExpression={setRegexExpression}
            setRuleRegexFlags={setRegexFlags}
            setRuleGroupOperator={setGroupOperator}
            setRuleGroupChildRuleIds={setGroupChildRuleIds}
            feeds={sortedFeeds}
            rules={rules}
            editingRuleId={isEditing ? id : undefined}
            onCreateRule={handleCreateRule}
            onUpdateRule={(payload: RuleUpdate) => handleUpdateRule(id, payload)}
            onDeleteRule={async () => {
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
          <RuleTable
            rules={rules}
            feeds={feeds}
            isLoading={rulesLoading || feedsLoading}
            allowEdit
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
