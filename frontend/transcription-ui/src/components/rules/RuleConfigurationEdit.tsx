import { useState } from 'react';

import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import RuleIcon from '@mui/icons-material/Rule';
import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Checkbox from '@mui/material/Checkbox';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import FormControlLabel from '@mui/material/FormControlLabel';
import FormHelperText from '@mui/material/FormHelperText';
import Grid from '@mui/material/Grid';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import InputLabel from '@mui/material/InputLabel';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import Stack from '@mui/material/Stack';
import Switch from '@mui/material/Switch';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  EvaluationType,
  Feed,
  LogicalOperator,
  Rule,
  RuleCreate,
  RuleUpdate,
  ScopeLevel,
} from '@transcription/common';

const EVALUATION_TYPE_OPTIONS: {
  value: EvaluationType;
  label: string;
}[] = [
  {
    value: 'KEYWORD_MATCH',
    label: 'Keyword Match',
  },
  {
    value: 'REGEX_MATCH',
    label: 'Regex Match',
  },
  {
    value: 'RULE_GROUP',
    label: 'Rule Group',
  },
];

interface RuleConfigurationEditProps {
  isEditing: boolean;
  ruleName: string;
  ruleDescription: string;
  ruleIsActive: boolean;
  ruleScopeLevel: ScopeLevel;
  ruleTargetFeeds: string[];
  ruleEvaluationType: EvaluationType;
  ruleKeywordOperator: LogicalOperator;
  ruleKeywords: string[];
  ruleKeywordCaseSensitive: boolean;
  ruleRegexExpression: string;
  ruleRegexFlags: string;
  ruleGroupOperator: LogicalOperator;
  ruleGroupChildRuleIds: string[];

  setRuleName: (val: string) => void;
  setRuleDescription: (val: string) => void;
  setRuleIsActive: (val: boolean) => void;
  setRuleScopeLevel: (val: ScopeLevel) => void;
  setRuleTargetFeeds: (val: string[]) => void;
  setRuleEvaluationType: (val: EvaluationType) => void;
  setRuleKeywordOperator: (val: LogicalOperator) => void;
  setRuleKeywords: (val: string[]) => void;
  setRuleKeywordCaseSensitive: (val: boolean) => void;
  setRuleRegexExpression: (val: string) => void;
  setRuleRegexFlags: (val: string) => void;
  setRuleGroupOperator: (val: LogicalOperator) => void;
  setRuleGroupChildRuleIds: (val: string[]) => void;

  feeds: Feed[];
  rules: Rule[];
  editingRuleId?: string;

  onCreateRule: (payload: RuleCreate) => Promise<void>;
  onUpdateRule: (payload: RuleUpdate) => Promise<void>;
  onDeleteRule?: () => Promise<void>;
  onCancel: () => void;
  isSubmitting: boolean;
}

export function RuleConfigurationEdit({
  isEditing,
  ruleName,
  ruleDescription,
  ruleIsActive,
  ruleScopeLevel,
  ruleTargetFeeds,
  ruleEvaluationType,
  ruleKeywordOperator,
  ruleKeywords,
  ruleKeywordCaseSensitive,
  ruleRegexExpression,
  ruleRegexFlags,
  ruleGroupOperator,
  ruleGroupChildRuleIds,

  setRuleName,
  setRuleDescription,
  setRuleIsActive,
  setRuleScopeLevel,
  setRuleTargetFeeds,
  setRuleEvaluationType,
  setRuleKeywordOperator,
  setRuleKeywords,
  setRuleKeywordCaseSensitive,
  setRuleRegexExpression,
  setRuleRegexFlags,
  setRuleGroupOperator,
  setRuleGroupChildRuleIds,

  feeds,
  rules,
  editingRuleId,
  onCreateRule,
  onUpdateRule,
  onDeleteRule,
  onCancel,
  isSubmitting,
}: RuleConfigurationEditProps) {
  const [newKeyword, setNewKeyword] = useState('');
  const [validationErrors, setValidationErrors] = useState<
    Record<string, string>
  >({});
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const menuOpen = Boolean(menuAnchorEl);
  const [confirmRuleName, setConfirmRuleName] = useState('');

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setMenuAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setMenuAnchorEl(null);
  };

  const handleDeleteClick = () => {
    handleMenuClose();
    setConfirmRuleName('');
    setIsDeleteDialogOpen(true);
  };

  const handleAddKeyword = () => {
    const word = newKeyword.trim();
    if (!word) return;

    // Handle comma-separated input
    const wordsToAdd = word
      .split(',')
      .map((w) => w.trim())
      .filter((w) => w.length > 0);

    const updatedKeywords = [...ruleKeywords];
    let hasDuplicate = false;

    for (const w of wordsToAdd) {
      if (!updatedKeywords.includes(w)) {
        updatedKeywords.push(w);
      } else {
        hasDuplicate = true;
      }
    }

    if (hasDuplicate) {
      setValidationErrors((prev) => ({
        ...prev,
        keywords: 'Some keywords were already added.',
      }));
    } else {
      setValidationErrors((prev) => {
        const copy = { ...prev };
        delete copy.keywords;
        return copy;
      });
    }

    setRuleKeywords(updatedKeywords);
    setNewKeyword('');
  };

  const handleRemoveKeyword = (wordToRemove: string) => {
    setRuleKeywords(ruleKeywords.filter((w) => w !== wordToRemove));
  };

  const handleKeywordKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleAddKeyword();
    }
  };

  const validateForm = (): Record<string, string> => {
    const errors: Record<string, string> = {};

    if (!ruleName.trim()) {
      errors.name = 'Rule name is required.';
    }

    if (ruleScopeLevel === 'FEED_SPECIFIC' && ruleTargetFeeds.length === 0) {
      errors.feeds = 'At least one target feed must be selected for FEED_SPECIFIC scope.';
    }

    if (ruleEvaluationType === 'KEYWORD_MATCH') {
      const activeKeywords = [...ruleKeywords];
      if (newKeyword.trim()) {
        const tempWords = newKeyword
          .split(',')
          .map((w) => w.trim())
          .filter((w) => w.length > 0);
        for (const tw of tempWords) {
          if (!activeKeywords.includes(tw)) activeKeywords.push(tw);
        }
      }

      if (activeKeywords.length === 0) {
        errors.keywords = 'At least one keyword is required for Keyword Match rules.';
      }
    } else if (ruleEvaluationType === 'REGEX_MATCH') {
      if (!ruleRegexExpression.trim()) {
        errors.regexExpression = 'Regex expression is required.';
      }
      try {
        new RegExp(ruleRegexExpression.trim());
      } catch (err) {
        errors.regexExpression = `Invalid regex expression: ${(err as Error).message}`;
      }
    } else if (ruleEvaluationType === 'RULE_GROUP') {
      if (ruleGroupChildRuleIds.length === 0) {
        errors.childRules = 'At least one child rule must be selected for Rule Group.';
      }
    }

    return errors;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const errors = validateForm();

    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setValidationErrors({});

    // Include the in-progress keyword if exists
    let finalKeywords = [...ruleKeywords];
    if (ruleEvaluationType === 'KEYWORD_MATCH' && newKeyword.trim()) {
      const word = newKeyword.trim();
      const wordsToAdd = word
        .split(',')
        .map((w) => w.trim())
        .filter((w) => w.length > 0);
      for (const w of wordsToAdd) {
        if (!finalKeywords.includes(w)) {
          finalKeywords.push(w);
        }
      }
      setRuleKeywords(finalKeywords);
      setNewKeyword('');
    }

    // Build Conditions
    let conditionsPayload: any;
    if (ruleEvaluationType === 'KEYWORD_MATCH') {
      conditionsPayload = {
        evaluationType: 'KEYWORD_MATCH',
        operator: ruleKeywordOperator,
        keywords: finalKeywords,
        caseSensitive: ruleKeywordCaseSensitive,
      };
    } else if (ruleEvaluationType === 'REGEX_MATCH') {
      conditionsPayload = {
        evaluationType: 'REGEX_MATCH',
        expression: ruleRegexExpression.trim(),
        flags: ruleRegexFlags.trim(),
      };
    } else if (ruleEvaluationType === 'RULE_GROUP') {
      conditionsPayload = {
        evaluationType: 'RULE_GROUP',
        operator: ruleGroupOperator,
        childRuleIds: ruleGroupChildRuleIds,
      };
    }

    const scopePayload = {
      level: ruleScopeLevel,
      targetFeeds: ruleScopeLevel === 'GLOBAL' ? [] : ruleTargetFeeds,
    };

    try {
      if (isEditing && editingRuleId) {
        const payload: RuleUpdate = {
          ruleName: ruleName.trim(),
          description: ruleDescription.trim() || undefined,
          isActive: ruleIsActive,
          scope: scopePayload,
          conditions: conditionsPayload,
        };
        await onUpdateRule(payload);
      } else {
        const payload: RuleCreate = {
          ruleName: ruleName.trim(),
          description: ruleDescription.trim() || undefined,
          isActive: ruleIsActive,
          scope: scopePayload,
          conditions: conditionsPayload,
        };
        await onCreateRule(payload);
      }
    } catch {
      // Errors propagated in mutate onError
    }
  };

  const handleDeleteConfirm = async () => {
    setConfirmRuleName('');
    setIsDeleteDialogOpen(false);
    if (onDeleteRule) {
      await onDeleteRule();
    }
  };

  // Filter out the rule itself if in edit mode to avoid self-reference in groups
  const eligibleChildRules = rules.filter(
    (r) => !isEditing || r.ruleId !== editingRuleId
  );

  return (
    <Card
      variant="outlined"
      data-testid="rule-config-card"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      <Box
        sx={{
          p: 3,
          color: isEditing ? 'warning.contrastText' : 'primary.contrastText',
          bgcolor: isEditing ? 'warning.main' : 'primary.main',
          flexShrink: 0,
        }}
      >
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          {isEditing ? `Edit Rule: ${ruleName}` : 'Create New Rule'}
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
          <Stack spacing={1} sx={{ display: 'flex', alignItems: 'left', justifyContent: 'left'}}>
            <TextField
              fullWidth
              label="Rule Name"
              size="small"
              variant="outlined"
              placeholder="Critical Keyword Rule"
              value={ruleName}
              onChange={(e) => setRuleName(e.target.value)}
              error={!!validationErrors.name}
              helperText={validationErrors.name || 'Descriptive name for the rule'}
              disabled={isSubmitting}
            />

            <TextField
              fullWidth
              label="Description (Optional)"
              size="small"
              variant="outlined"
              placeholder="Matches emergency evacuation triggers"
              value={ruleDescription}
              onChange={(e) => setRuleDescription(e.target.value)}
              disabled={isSubmitting}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={ruleIsActive}
                  onChange={(e) => setRuleIsActive(e.target.checked)}
                  disabled={isSubmitting}
                />
              }
              label="Is Active"
            />

            <Divider sx={{ my: 1 }} />

            {/* Scope Section */}
            <Box>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                Scope Configuration
              </Typography>
              <Grid container spacing={2}>
                <Grid size={{ xs: 12, sm: 6 }}>
                  <FormControl fullWidth size="small" disabled={isSubmitting}>
                    <InputLabel id="scope-level-label">Scope Level</InputLabel>
                    <Select
                      labelId="scope-level-label"
                      value={ruleScopeLevel}
                      label="Scope Level"
                      onChange={(e) => setRuleScopeLevel(e.target.value as ScopeLevel)}
                    >
                      <MenuItem value="GLOBAL">Global</MenuItem>
                      <MenuItem value="FEED_SPECIFIC">Feed Specific</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>

                {ruleScopeLevel === 'FEED_SPECIFIC' ? (
                  <Grid size={{ xs: 12 }}>
                    <Autocomplete
                      multiple
                      options={feeds}
                      getOptionLabel={(option) => option.name}
                      value={feeds.filter((f) => ruleTargetFeeds.includes(f.id))}
                      onChange={(_, selectedOptions) => {
                        setRuleTargetFeeds(selectedOptions.map((f) => f.id));
                      }}
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          label="Target Feeds"
                          size="small"
                          error={!!validationErrors.feeds}
                          helperText={validationErrors.feeds}
                        />
                      )}
                      renderOption={(props, option) => {
                        const { key, ...optionProps } = props;
                        return (
                          <Box key={key} component="li" {...optionProps}>
                            <Stack>
                              <Typography>{option.name}</Typography>
                              <Typography variant="caption" sx={{ color: 'text.secondary' }}>Source ID: {option.sourceFeedId}</Typography>
                            </Stack>
                          </Box>
                        );
                      }}
                      disableCloseOnSelect
                      disabled={isSubmitting}
                    />
                  </Grid>
                ) : null}
              </Grid>
            </Box>

            <Divider sx={{ my: 1 }} />

            {/* Conditions Section */}
            <Box>
              <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
                <RuleIcon fontSize="small" color="action" />
                <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                  Conditions
                </Typography>
              </Stack>

              {/* Evaluation Type */}
              <FormControl fullWidth size="small" sx={{ mb: 2.5 }} disabled={isSubmitting}>
                <InputLabel id="evaluation-type-label">Evaluation Type</InputLabel>
                <Select
                  labelId="evaluation-type-label"
                  value={ruleEvaluationType}
                  label="Evaluation Type"
                  onChange={(e) => setRuleEvaluationType(e.target.value as EvaluationType)}
                >
                  {EVALUATION_TYPE_OPTIONS.map((opt) => (
                    <MenuItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              {/* Keywords Form */}
              {ruleEvaluationType === 'KEYWORD_MATCH' ? (
                <Stack spacing={2}>
                  <Grid container spacing={2}>
                    <Grid size={{ xs: 12, sm: 6 }}>
                      <FormControl fullWidth size="small" disabled={isSubmitting}>
                        <InputLabel id="keyword-operator-label">Logical Operator</InputLabel>
                        <Select
                          labelId="keyword-operator-label"
                          value={ruleKeywordOperator}
                          label="Logical Operator"
                          onChange={(e) => setRuleKeywordOperator(e.target.value as LogicalOperator)}
                        >
                          <MenuItem value="ANY">ANY (OR)</MenuItem>
                          <MenuItem value="ALL">ALL (AND)</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid size={{ xs: 12, sm: 6 }}>
                      <FormControlLabel
                        control={
                          <Checkbox
                            checked={ruleKeywordCaseSensitive}
                            onChange={(e) => setRuleKeywordCaseSensitive(e.target.checked)}
                            disabled={isSubmitting}
                          />
                        }
                        label="Case Sensitive"
                      />
                    </Grid>
                  </Grid>

                  {/* Add Keywords */}
                  <Stack direction="row" spacing={1.5} sx={{ alignItems: 'flex-start' }}>
                    <TextField
                      fullWidth
                      size="small"
                      label="Add Keywords"
                      placeholder="fire, evacuation, dispatch"
                      value={newKeyword}
                      onChange={(e) => setNewKeyword(e.target.value)}
                      onKeyDown={handleKeywordKeyPress}
                      error={!!validationErrors.keywords}
                      helperText={validationErrors.keywords || 'Separate multiple keywords with commas'}
                      disabled={isSubmitting}
                    />
                    <Button
                      variant="outlined"
                      onClick={handleAddKeyword}
                      disabled={isSubmitting}
                      startIcon={<AddIcon fontSize="small" />}
                      sx={{ textTransform: 'none', height: 40 }}
                    >
                      Add
                    </Button>
                  </Stack>

                  {/* Chips Display */}
                  <Box
                    sx={{
                      p: 1.5,
                      borderRadius: 1.5,
                      border: '1px dashed',
                      borderColor: 'divider',
                      bgcolor: 'background.default',
                      display: 'flex',
                      flexWrap: 'wrap',
                      gap: 1,
                    }}
                  >
                    {ruleKeywords.length === 0 ? (
                      <Typography variant="body2" color="text.secondary" sx={{ py: 1, mx: 'auto', fontStyle: 'italic' }}>
                        No keywords added yet.
                      </Typography>
                    ) : (
                      ruleKeywords.map((kw, idx) => (
                        <Chip
                          key={idx}
                          label={kw}
                          onDelete={() => handleRemoveKeyword(kw)}
                          disabled={isSubmitting}
                          size="small"
                        />
                      ))
                    )}
                  </Box>
                </Stack>
              ) : null}

              {/* Regex Form */}
              {ruleEvaluationType === 'REGEX_MATCH' ? (
                <Stack spacing={2}>
                  <TextField
                    fullWidth
                    size="small"
                    label="Regex Expression"
                    placeholder="evac(uation)?"
                    value={ruleRegexExpression}
                    onChange={(e) => setRuleRegexExpression(e.target.value)}
                    error={!!validationErrors.regexExpression}
                    helperText={validationErrors.regexExpression}
                    disabled={isSubmitting}
                  />
                  <TextField
                    fullWidth
                    size="small"
                    label="Flags"
                    placeholder="i"
                    value={ruleRegexFlags}
                    onChange={(e) => setRuleRegexFlags(e.target.value)}
                    disabled={isSubmitting}
                  />
                </Stack>
              ) : null}

              {/* Rule Group Form */}
              {ruleEvaluationType === 'RULE_GROUP' ? (
                <Stack spacing={2}>
                  <FormControl fullWidth size="small" disabled={isSubmitting}>
                    <InputLabel id="group-operator-label">Logical Operator</InputLabel>
                    <Select
                      labelId="group-operator-label"
                      value={ruleGroupOperator}
                      label="Logical Operator"
                      onChange={(e) => setRuleGroupOperator(e.target.value as LogicalOperator)}
                    >
                      <MenuItem value="ANY">ANY (OR)</MenuItem>
                      <MenuItem value="ALL">ALL (AND)</MenuItem>
                    </Select>
                  </FormControl>

                  <Autocomplete
                    multiple
                    options={eligibleChildRules}
                    getOptionLabel={(option) => option.ruleName}
                    value={eligibleChildRules.filter((r) => ruleGroupChildRuleIds.includes(r.ruleId))}
                    onChange={(_, selectedOptions) => {
                      setRuleGroupChildRuleIds(selectedOptions.map((r) => r.ruleId));
                    }}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Child Rules"
                        size="small"
                        error={!!validationErrors.childRules}
                        helperText={validationErrors.childRules || 'Rules to group together'}
                      />
                    )}
                    disableCloseOnSelect
                    disabled={isSubmitting}
                  />
                </Stack>
              ) : null}
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
                    'Create Rule'
                  )}
                </Button>

                {isEditing && (
                  <>
                    <IconButton
                      aria-label="rule actions"
                      aria-controls={menuOpen ? 'rule-actions-menu' : undefined}
                      aria-haspopup="true"
                      aria-expanded={menuOpen ? 'true' : undefined}
                      onClick={handleMenuOpen}
                      disabled={isSubmitting}
                      size="small"
                    >
                      <MoreVertIcon />
                    </IconButton>
                    <Menu
                      id="rule-actions-menu"
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
                      <MenuItem
                        onClick={handleDeleteClick}
                        disabled={isSubmitting}
                        sx={{ color: 'error.main' }}
                      >
                        Delete rule
                      </MenuItem>
                    </Menu>
                  </>
                )}
              </Box>
            </Box>
          </Stack>
        </Box>
      </CardContent>

      <Dialog
        open={isDeleteDialogOpen}
        onClose={() => {
          setConfirmRuleName('');
          setIsDeleteDialogOpen(false);
        }}
        aria-labelledby="delete-rule-dialog-title"
        aria-describedby="delete-rule-dialog-description"
      >
        <DialogTitle id="delete-rule-dialog-title">
          Verify Rule Deletion
        </DialogTitle>
        <DialogContent>
          <DialogContentText id="delete-rule-dialog-description" sx={{ mb: 2 }}>
            Are you sure you want to delete the rule "{ruleName}"? This action cannot be undone.
          </DialogContentText>
          <DialogContentText sx={{ mb: 1, fontWeight: 'bold' }}>
            To confirm, type the Rule Name "{ruleName}" below:
          </DialogContentText>
          <TextField
            fullWidth
            size="small"
            variant="outlined"
            value={confirmRuleName}
            onChange={(e) => setConfirmRuleName(e.target.value)}
            placeholder={ruleName}
            disabled={isSubmitting}
          />
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => {
              setConfirmRuleName('');
              setIsDeleteDialogOpen(false);
            }}
            color="primary"
            disabled={isSubmitting}
            sx={{ textTransform: 'none' }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleDeleteConfirm}
            color="error"
            variant="contained"
            disabled={confirmRuleName !== ruleName || isSubmitting}
            sx={{ textTransform: 'none' }}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </Card>
  );
}

export default RuleConfigurationEdit;
