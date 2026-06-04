import { useState } from 'react';

import AddIcon from '@mui/icons-material/Add';
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
import Grid from '@mui/material/Grid';
import IconButton from '@mui/material/IconButton';
import InputLabel from '@mui/material/InputLabel';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type {
  EvaluationType,
  Feed,
  LogicalOperator,
  Rule,
  RuleConditions,
  RuleCreate,
  RuleUpdate,
  ScopeLevel,
} from '@transcription/common';

import { buildRulePayload, validateRule } from '../../utils/validationUtils';

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
  editingRule: RuleCreate;
  setEditingRule: React.Dispatch<React.SetStateAction<RuleCreate>>;
  editingRuleId?: string;
  feeds: Feed[];
  rules: Rule[];
  onCreateRule: (payload: RuleCreate) => void;
  onUpdateRule: (payload: RuleUpdate) => void;
  onDeleteRule: () => void;
  onCancel: () => void;
  isSubmitting: boolean;
}

export function RuleConfigurationEdit({
  isEditing,
  editingRule,
  setEditingRule,
  editingRuleId,
  feeds,
  rules,
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
  const [isToggleActiveDialogOpen, setIsToggleActiveDialogOpen] =
    useState(false);
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
    setIsDeleteDialogOpen(true);
  };

  const handleToggleActiveClick = () => {
    handleMenuClose();
    setIsToggleActiveDialogOpen(true);
  };

  const handleToggleActiveConfirm = () => {
    setIsToggleActiveDialogOpen(false);
    const payload = buildRulePayload(
      {
        ...editingRule,
        isActive: !editingRule.isActive,
      },
      newKeyword
    );
    onUpdateRule(payload);
  };

  const handleAddKeyword = () => {
    const word = newKeyword.trim();
    if (!word) return;

    // Handle comma-separated input
    const wordsToAdd = word
      .split(',')
      .map((w) => w.trim())
      .filter((w) => w.length > 0);

    setEditingRule((prev) => {
      if (prev.conditions.evaluationType !== 'KEYWORD_MATCH') return prev;
      const currentKeywords = prev.conditions.keywords;
      const updatedKeywords = [...currentKeywords];
      let hasDuplicate = false;

      for (const w of wordsToAdd) {
        if (!updatedKeywords.includes(w)) {
          updatedKeywords.push(w);
        } else {
          hasDuplicate = true;
        }
      }

      if (hasDuplicate) {
        setValidationErrors((prevErrors) => ({
          ...prevErrors,
          keywords: 'Some keywords were already added.',
        }));
      } else {
        setValidationErrors((prevErrors) => {
          const copy = { ...prevErrors };
          delete copy.keywords;
          return copy;
        });
      }

      return {
        ...prev,
        conditions: {
          ...prev.conditions,
          keywords: updatedKeywords,
        },
      };
    });
    setNewKeyword('');
  };

  const handleRemoveKeyword = (wordToRemove: string) => {
    setEditingRule((prev) => {
      if (prev.conditions.evaluationType !== 'KEYWORD_MATCH') return prev;
      return {
        ...prev,
        conditions: {
          ...prev.conditions,
          keywords: prev.conditions.keywords.filter((w) => w !== wordToRemove),
        },
      };
    });
  };

  const handleKeywordKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleAddKeyword();
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const errors = validateRule(editingRule, newKeyword);

    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setValidationErrors({});

    const payload = buildRulePayload(editingRule, newKeyword);

    if (isEditing) {
      onUpdateRule(payload);
    } else {
      onCreateRule(payload);
      setNewKeyword('');
    }
  };

  const handleDeleteConfirm = () => {
    setIsDeleteDialogOpen(false);
    onDeleteRule();
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
          {isEditing ? `Edit Rule: ${editingRule.ruleName}` : 'Create New Rule'}
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
          <Stack
            spacing={2}
            sx={{ display: 'flex', alignItems: 'left', justifyContent: 'left' }}
          >
            <RuleBasicInfoSection
              editingRule={editingRule}
              setEditingRule={setEditingRule}
              validationErrors={validationErrors}
              isSubmitting={isSubmitting}
            />

            <Divider sx={{ my: 1 }} />

            <RuleScopeSection
              editingRule={editingRule}
              setEditingRule={setEditingRule}
              feeds={feeds}
              validationErrors={validationErrors}
              isSubmitting={isSubmitting}
            />

            <Divider sx={{ my: 1 }} />

            <RuleConditionsSection
              editingRule={editingRule}
              setEditingRule={setEditingRule}
              eligibleChildRules={eligibleChildRules}
              newKeyword={newKeyword}
              setNewKeyword={setNewKeyword}
              handleAddKeyword={handleAddKeyword}
              handleRemoveKeyword={handleRemoveKeyword}
              handleKeywordKeyPress={handleKeywordKeyPress}
              validationErrors={validationErrors}
              isSubmitting={isSubmitting}
            />

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
                        onClick={handleToggleActiveClick}
                        disabled={isSubmitting}
                      >
                        {editingRule.isActive ? 'Disable rule' : 'Enable rule'}
                      </MenuItem>
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

      <RuleActionConfirmDialog
        isOpen={isDeleteDialogOpen}
        onClose={() => setIsDeleteDialogOpen(false)}
        title="Verify Rule Deletion"
        description={`Are you sure you want to delete the rule "${editingRule.ruleName}"? This action cannot be undone.`}
        confirmLabel="Delete"
        confirmColor="error"
        ruleName={editingRule.ruleName}
        onConfirm={handleDeleteConfirm}
        isSubmitting={isSubmitting}
        requireNameConfirmation
      />

      <RuleActionConfirmDialog
        isOpen={isToggleActiveDialogOpen}
        onClose={() => setIsToggleActiveDialogOpen(false)}
        title={
          editingRule.isActive
            ? 'Verify Disabling Rule'
            : 'Verify Enabling Rule'
        }
        description={
          editingRule.isActive
            ? `Are you sure you want to disable the rule "${editingRule.ruleName}"?`
            : `Are you sure you want to enable the rule "${editingRule.ruleName}"?`
        }
        confirmLabel={editingRule.isActive ? 'Disable' : 'Enable'}
        ruleName={editingRule.ruleName}
        onConfirm={handleToggleActiveConfirm}
        isSubmitting={isSubmitting}
      />
    </Card>
  );
}

interface RuleBasicInfoSectionProps {
  editingRule: RuleCreate;
  setEditingRule: React.Dispatch<React.SetStateAction<RuleCreate>>;
  validationErrors: Record<string, string>;
  isSubmitting: boolean;
}

function RuleBasicInfoSection({
  editingRule,
  setEditingRule,
  validationErrors,
  isSubmitting,
}: RuleBasicInfoSectionProps) {
  return (
    <>
      <TextField
        fullWidth
        label="Rule Name"
        size="small"
        variant="outlined"
        placeholder="Critical Keyword Rule"
        value={editingRule.ruleName}
        onChange={(e) =>
          setEditingRule((prev) => ({
            ...prev,
            ruleName: e.target.value,
          }))
        }
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
        value={editingRule.description || ''}
        onChange={(e) =>
          setEditingRule((prev) => ({
            ...prev,
            description: e.target.value,
          }))
        }
        disabled={isSubmitting}
      />
    </>
  );
}

interface RuleScopeSectionProps {
  editingRule: RuleCreate;
  setEditingRule: React.Dispatch<React.SetStateAction<RuleCreate>>;
  feeds: Feed[];
  validationErrors: Record<string, string>;
  isSubmitting: boolean;
}

function RuleScopeSection({
  editingRule,
  setEditingRule,
  feeds,
  validationErrors,
  isSubmitting,
}: RuleScopeSectionProps) {
  return (
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
              value={editingRule.scope.level}
              label="Scope Level"
              onChange={(e) =>
                setEditingRule((prev) => ({
                  ...prev,
                  scope: {
                    ...prev.scope,
                    level: e.target.value as ScopeLevel,
                  },
                }))
              }
            >
              <MenuItem value="GLOBAL">Global</MenuItem>
              <MenuItem value="FEED_SPECIFIC">Feed Specific</MenuItem>
            </Select>
          </FormControl>
        </Grid>

        {editingRule.scope.level === 'FEED_SPECIFIC' ? (
          <Grid size={{ xs: 12 }}>
            <Autocomplete
              multiple
              options={feeds}
              getOptionLabel={(option) => option.name}
              value={feeds.filter((f) =>
                editingRule.scope.targetFeeds?.includes(f.id)
              )}
              onChange={(_, selectedOptions) => {
                setEditingRule((prev) => ({
                  ...prev,
                  scope: {
                    ...prev.scope,
                    targetFeeds: selectedOptions.map((f) => f.id),
                  },
                }));
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
                      <Typography
                        variant="caption"
                        sx={{ color: 'text.secondary' }}
                      >
                        Source ID: {option.sourceFeedId}
                      </Typography>
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
  );
}

interface RuleConditionsSectionProps {
  editingRule: RuleCreate;
  setEditingRule: React.Dispatch<React.SetStateAction<RuleCreate>>;
  eligibleChildRules: Rule[];
  newKeyword: string;
  setNewKeyword: React.Dispatch<React.SetStateAction<string>>;
  handleAddKeyword: () => void;
  handleRemoveKeyword: (kw: string) => void;
  handleKeywordKeyPress: (e: React.KeyboardEvent) => void;
  validationErrors: Record<string, string>;
  isSubmitting: boolean;
}

function RuleConditionsSection({
  editingRule,
  setEditingRule,
  eligibleChildRules,
  newKeyword,
  setNewKeyword,
  handleAddKeyword,
  handleRemoveKeyword,
  handleKeywordKeyPress,
  validationErrors,
  isSubmitting,
}: RuleConditionsSectionProps) {
  const isKeywordMatch =
    editingRule.conditions.evaluationType === 'KEYWORD_MATCH';
  const isRegexMatch = editingRule.conditions.evaluationType === 'REGEX_MATCH';
  const isRuleGroup = editingRule.conditions.evaluationType === 'RULE_GROUP';

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
        <RuleIcon fontSize="small" color="action" />
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
          Conditions
        </Typography>
      </Stack>

      <FormControl
        fullWidth
        size="small"
        sx={{ mb: 2 }}
        disabled={isSubmitting}
      >
        <InputLabel id="evaluation-type-label">Evaluation Type</InputLabel>
        <Select
          labelId="evaluation-type-label"
          value={editingRule.conditions.evaluationType}
          label="Evaluation Type"
          onChange={(e) => {
            const newType = e.target.value as EvaluationType;
            setEditingRule((prev) => {
              let nextConditions: RuleConditions;
              if (newType === 'KEYWORD_MATCH') {
                nextConditions = {
                  evaluationType: 'KEYWORD_MATCH',
                  operator: 'ANY',
                  keywords: [],
                  caseSensitive: false,
                };
              } else if (newType === 'REGEX_MATCH') {
                nextConditions = {
                  evaluationType: 'REGEX_MATCH',
                  expression: '',
                  flags: '',
                };
              } else {
                nextConditions = {
                  evaluationType: 'RULE_GROUP',
                  operator: 'ANY',
                  childRuleIds: [],
                };
              }
              return {
                ...prev,
                conditions: nextConditions,
              };
            });
          }}
        >
          {EVALUATION_TYPE_OPTIONS.map((opt) => (
            <MenuItem key={opt.value} value={opt.value}>
              {opt.label}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {isKeywordMatch ? (
        <Stack spacing={2}>
          <Grid container spacing={2}>
            <Grid size={{ xs: 12, sm: 6 }}>
              <FormControl fullWidth size="small" disabled={isSubmitting}>
                <InputLabel id="keyword-operator-label">
                  Logical Operator
                </InputLabel>
                <Select
                  labelId="keyword-operator-label"
                  value={
                    editingRule.conditions.evaluationType === 'KEYWORD_MATCH'
                      ? editingRule.conditions.operator
                      : 'ANY'
                  }
                  label="Logical Operator"
                  onChange={(e) =>
                    setEditingRule((prev) => {
                      if (prev.conditions.evaluationType !== 'KEYWORD_MATCH')
                        return prev;
                      return {
                        ...prev,
                        conditions: {
                          ...prev.conditions,
                          operator: e.target.value as LogicalOperator,
                        },
                      };
                    })
                  }
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
                    checked={
                      editingRule.conditions.evaluationType === 'KEYWORD_MATCH'
                        ? editingRule.conditions.caseSensitive
                        : false
                    }
                    onChange={(e) =>
                      setEditingRule((prev) => {
                        if (prev.conditions.evaluationType !== 'KEYWORD_MATCH')
                          return prev;
                        return {
                          ...prev,
                          conditions: {
                            ...prev.conditions,
                            caseSensitive: e.target.checked,
                          },
                        };
                      })
                    }
                    disabled={isSubmitting}
                  />
                }
                label="Case Sensitive"
              />
            </Grid>
          </Grid>

          <Stack direction="row" spacing={1} sx={{ alignItems: 'flex-start' }}>
            <TextField
              fullWidth
              size="small"
              label="Add Keywords"
              placeholder="fire, evacuation, dispatch"
              value={newKeyword}
              onChange={(e) => setNewKeyword(e.target.value)}
              onKeyDown={handleKeywordKeyPress}
              error={!!validationErrors.keywords}
              helperText={
                validationErrors.keywords ||
                'Separate multiple keywords with commas'
              }
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
            {editingRule.conditions.evaluationType === 'KEYWORD_MATCH' &&
            editingRule.conditions.keywords.length === 0 ? (
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ py: 1, mx: 'auto', fontStyle: 'italic' }}
              >
                No keywords added yet.
              </Typography>
            ) : (
              editingRule.conditions.evaluationType === 'KEYWORD_MATCH' &&
              editingRule.conditions.keywords.map((kw, idx) => (
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

      {isRegexMatch ? (
        <Stack spacing={2}>
          <TextField
            fullWidth
            size="small"
            label="Regex Expression"
            placeholder=""
            value={
              editingRule.conditions.evaluationType === 'REGEX_MATCH'
                ? editingRule.conditions.expression
                : ''
            }
            onChange={(e) =>
              setEditingRule((prev) => {
                if (prev.conditions.evaluationType !== 'REGEX_MATCH')
                  return prev;
                return {
                  ...prev,
                  conditions: {
                    ...prev.conditions,
                    expression: e.target.value,
                  },
                };
              })
            }
            error={!!validationErrors.regexExpression}
            helperText={validationErrors.regexExpression}
            disabled={isSubmitting}
          />
          <TextField
            fullWidth
            size="small"
            label="Flags"
            placeholder=""
            value={
              editingRule.conditions.evaluationType === 'REGEX_MATCH'
                ? editingRule.conditions.flags
                : ''
            }
            onChange={(e) =>
              setEditingRule((prev) => {
                if (prev.conditions.evaluationType !== 'REGEX_MATCH')
                  return prev;
                return {
                  ...prev,
                  conditions: {
                    ...prev.conditions,
                    flags: e.target.value,
                  },
                };
              })
            }
            disabled={isSubmitting}
          />
        </Stack>
      ) : null}

      {isRuleGroup ? (
        <Stack spacing={2}>
          <FormControl fullWidth size="small" disabled={isSubmitting}>
            <InputLabel id="group-operator-label">Logical Operator</InputLabel>
            <Select
              labelId="group-operator-label"
              value={
                editingRule.conditions.evaluationType === 'RULE_GROUP'
                  ? editingRule.conditions.operator
                  : 'ANY'
              }
              label="Logical Operator"
              onChange={(e) =>
                setEditingRule((prev) => {
                  if (prev.conditions.evaluationType !== 'RULE_GROUP')
                    return prev;
                  return {
                    ...prev,
                    conditions: {
                      ...prev.conditions,
                      operator: e.target.value as LogicalOperator,
                    },
                  };
                })
              }
            >
              <MenuItem value="ANY">ANY (OR)</MenuItem>
              <MenuItem value="ALL">ALL (AND)</MenuItem>
            </Select>
          </FormControl>

          <Autocomplete
            multiple
            options={eligibleChildRules}
            getOptionLabel={(option) => option.ruleName}
            value={eligibleChildRules.filter(
              (r) =>
                editingRule.conditions.evaluationType === 'RULE_GROUP' &&
                editingRule.conditions.childRuleIds?.includes(r.ruleId)
            )}
            onChange={(_, selectedOptions) => {
              setEditingRule((prev) => {
                if (prev.conditions.evaluationType !== 'RULE_GROUP')
                  return prev;
                return {
                  ...prev,
                  conditions: {
                    ...prev.conditions,
                    childRuleIds: selectedOptions.map((r) => r.ruleId),
                  },
                };
              });
            }}
            renderInput={(params) => (
              <TextField
                {...params}
                label="Child Rules"
                size="small"
                error={!!validationErrors.childRules}
                helperText={
                  validationErrors.childRules || 'Rules to group together'
                }
              />
            )}
            disableCloseOnSelect
            disabled={isSubmitting}
          />
        </Stack>
      ) : null}
    </Box>
  );
}

interface RuleActionConfirmDialogProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  description: string;
  confirmLabel: string;
  confirmColor?:
    | 'primary'
    | 'secondary'
    | 'error'
    | 'info'
    | 'success'
    | 'warning';
  ruleName: string;
  onConfirm: () => void;
  isSubmitting: boolean;
  requireNameConfirmation?: boolean;
}

function RuleActionConfirmDialog({
  isOpen,
  onClose,
  title,
  description,
  confirmLabel,
  confirmColor = 'primary',
  ruleName,
  onConfirm,
  isSubmitting,
  requireNameConfirmation = false,
}: RuleActionConfirmDialogProps) {
  const [confirmRuleName, setConfirmRuleName] = useState('');

  const handleClose = () => {
    setConfirmRuleName('');
    onClose();
  };

  const handleConfirmSubmit = () => {
    setConfirmRuleName('');
    onConfirm();
  };

  return (
    <Dialog
      open={isOpen}
      onClose={handleClose}
      aria-labelledby="rule-action-dialog-title"
      aria-describedby="rule-action-dialog-description"
    >
      <DialogTitle id="rule-action-dialog-title">{title}</DialogTitle>
      <DialogContent>
        <DialogContentText
          id="rule-action-dialog-description"
          sx={{ mb: requireNameConfirmation ? 2 : 0 }}
        >
          {description}
        </DialogContentText>
        {requireNameConfirmation && (
          <>
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
          </>
        )}
      </DialogContent>
      <DialogActions>
        <Button
          onClick={handleClose}
          color="primary"
          disabled={isSubmitting}
          sx={{ textTransform: 'none' }}
        >
          Cancel
        </Button>
        <Button
          onClick={handleConfirmSubmit}
          color={confirmColor}
          variant="contained"
          disabled={
            (requireNameConfirmation && confirmRuleName !== ruleName) ||
            isSubmitting
          }
          sx={{ textTransform: 'none' }}
        >
          {confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default RuleConfigurationEdit;
