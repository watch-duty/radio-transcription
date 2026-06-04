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
import Switch from '@mui/material/Switch';
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

import { validateRule } from '../../utils/validationUtils';

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

    // Include the in-progress keyword if exists
    const finalKeywords =
      editingRule.conditions.evaluationType === 'KEYWORD_MATCH'
        ? [...editingRule.conditions.keywords]
        : [];
    if (
      editingRule.conditions.evaluationType === 'KEYWORD_MATCH' &&
      newKeyword.trim()
    ) {
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
    }

    // Build Conditions
    let conditionsPayload: RuleConditions;
    if (editingRule.conditions.evaluationType === 'KEYWORD_MATCH') {
      conditionsPayload = {
        evaluationType: 'KEYWORD_MATCH',
        operator: editingRule.conditions.operator,
        keywords: finalKeywords,
        caseSensitive: editingRule.conditions.caseSensitive,
      };
    } else if (editingRule.conditions.evaluationType === 'REGEX_MATCH') {
      conditionsPayload = {
        evaluationType: 'REGEX_MATCH',
        expression: editingRule.conditions.expression.trim(),
        flags: editingRule.conditions.flags.trim(),
      };
    } else {
      conditionsPayload = {
        evaluationType: 'RULE_GROUP',
        operator: editingRule.conditions.operator,
        childRuleIds: editingRule.conditions.childRuleIds,
      };
    }

    const scopePayload = {
      level: editingRule.scope.level,
      targetFeeds:
        editingRule.scope.level === 'GLOBAL'
          ? []
          : editingRule.scope.targetFeeds,
    };

    if (isEditing) {
      const payload: RuleUpdate = {
        ruleName: editingRule.ruleName.trim(),
        description: editingRule.description?.trim() || undefined,
        isActive: editingRule.isActive,
        scope: scopePayload,
        conditions: conditionsPayload,
      };
      onUpdateRule(payload);
    } else {
      const payload: RuleCreate = {
        ruleName: editingRule.ruleName.trim(),
        description: editingRule.description?.trim() || undefined,
        isActive: editingRule.isActive,
        scope: scopePayload,
        conditions: conditionsPayload,
      };
      onCreateRule(payload);
      setNewKeyword('');
    }
  };

  const handleDeleteConfirm = () => {
    setConfirmRuleName('');
    setIsDeleteDialogOpen(false);
    onDeleteRule();
  };

  // Filter out the rule itself if in edit mode to avoid self-reference in groups
  const eligibleChildRules = rules.filter(
    (r) => !isEditing || r.ruleId !== editingRuleId
  );

  const isKeywordMatch =
    editingRule.conditions.evaluationType === 'KEYWORD_MATCH';
  const isRegexMatch = editingRule.conditions.evaluationType === 'REGEX_MATCH';
  const isRuleGroup = editingRule.conditions.evaluationType === 'RULE_GROUP';

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
              helperText={
                validationErrors.name || 'Descriptive name for the rule'
              }
              disabled={isSubmitting}
            />

            <TextField
              fullWidth
              label="Description (Optional)"
              size="small"
              variant="outlined"
              placeholder="Matches emergency evacuation triggers"
              value={editingRule.description}
              onChange={(e) =>
                setEditingRule((prev) => ({
                  ...prev,
                  description: e.target.value,
                }))
              }
              disabled={isSubmitting}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={editingRule.isActive ?? true}
                  onChange={(e) =>
                    setEditingRule((prev) => ({
                      ...prev,
                      isActive: e.target.checked,
                    }))
                  }
                  disabled={isSubmitting}
                />
              }
              label="Is Active"
              sx={{ alignSelf: 'flex-start' }}
            />

            <Divider sx={{ my: 1 }} />

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

            <Divider sx={{ my: 1 }} />

            <Box>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: 'center', mb: 2 }}
              >
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
                <InputLabel id="evaluation-type-label">
                  Evaluation Type
                </InputLabel>
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
                      <FormControl
                        fullWidth
                        size="small"
                        disabled={isSubmitting}
                      >
                        <InputLabel id="keyword-operator-label">
                          Logical Operator
                        </InputLabel>
                        <Select
                          labelId="keyword-operator-label"
                          value={
                            editingRule.conditions.evaluationType ===
                            'KEYWORD_MATCH'
                              ? editingRule.conditions.operator
                              : 'ANY'
                          }
                          label="Logical Operator"
                          onChange={(e) =>
                            setEditingRule((prev) => {
                              if (
                                prev.conditions.evaluationType !==
                                'KEYWORD_MATCH'
                              )
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
                              editingRule.conditions.evaluationType ===
                              'KEYWORD_MATCH'
                                ? editingRule.conditions.caseSensitive
                                : false
                            }
                            onChange={(e) =>
                              setEditingRule((prev) => {
                                if (
                                  prev.conditions.evaluationType !==
                                  'KEYWORD_MATCH'
                                )
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

                  <Stack
                    direction="row"
                    spacing={1}
                    sx={{ alignItems: 'flex-start' }}
                  >
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
                    {editingRule.conditions.evaluationType ===
                      'KEYWORD_MATCH' &&
                    editingRule.conditions.keywords.length === 0 ? (
                      <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ py: 1, mx: 'auto', fontStyle: 'italic' }}
                      >
                        No keywords added yet.
                      </Typography>
                    ) : (
                      editingRule.conditions.evaluationType ===
                        'KEYWORD_MATCH' &&
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
                    <InputLabel id="group-operator-label">
                      Logical Operator
                    </InputLabel>
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
                        editingRule.conditions.evaluationType ===
                          'RULE_GROUP' &&
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
                          validationErrors.childRules ||
                          'Rules to group together'
                        }
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
            Are you sure you want to delete the rule "{editingRule.ruleName}"?
            This action cannot be undone.
          </DialogContentText>
          <DialogContentText sx={{ mb: 1, fontWeight: 'bold' }}>
            To confirm, type the Rule Name "{editingRule.ruleName}" below:
          </DialogContentText>
          <TextField
            fullWidth
            size="small"
            variant="outlined"
            value={confirmRuleName}
            onChange={(e) => setConfirmRuleName(e.target.value)}
            placeholder={editingRule.ruleName}
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
            disabled={confirmRuleName !== editingRule.ruleName || isSubmitting}
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
