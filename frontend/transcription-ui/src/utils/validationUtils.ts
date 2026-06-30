import { SourceType } from '@transcription/common';
import type { RuleConditions, RuleCreate } from '@transcription/common';

/**
 * Validates whether a given string is a valid timezone.
 * NOTE: The 'system/timezone' tag is currently only recognized by the fire notifications collector.
 */
export function isValidTimezone(tz: string): boolean {
  try {
    Intl.DateTimeFormat(undefined, { timeZone: tz });
    return true;
  } catch {
    return false;
  }
}

/**
 * Validates a feed source ID based on its SourceType.
 * Returns an error message string if invalid, or null if valid.
 */
export function validateFeedSourceId(
  sourceType: SourceType,
  sourceId: string
): string | null {
  const trimmedId = sourceId.trim();
  if (!trimmedId) {
    return 'Source feed ID is required.';
  }

  switch (sourceType) {
    case SourceType.BCFY_CALLS:
      if (!/^\d+-\d+$/.test(trimmedId)) {
        return 'Must only contain numbers formatted as sid-talkgroup.';
      }
      break;
    case SourceType.BCFY_FEEDS:
      if (!/^\d+$/.test(trimmedId)) {
        return 'Must be a number.';
      }
      break;
    case SourceType.ECHO:
      if (!/^[a-zA-Z0-9_-]+$/.test(trimmedId)) {
        return 'Must only contain letters, numbers, and the following special characters: - _';
      }
      break;
    case SourceType.FIRE_NOTIFICATIONS:
      if (!/^[A-Z0-9_\-/()]+$/.test(trimmedId)) {
        return 'Must only contain uppercase letters, numbers, and the following special characters: / - ( )';
      }
      break;
    case SourceType.OPENMHZ:
      if (!/^\w+$/.test(trimmedId)) {
        return 'Must be alphanumeric.';
      }
      break;
    default:
      break;
  }

  return null;
}

/**
 * Validates a rule to ensure all required fields are populated correctly.
 *
 * @param rule The rule data to validate
 * @param inProgressKeyword Optional keyword string currently being typed in the input field
 * @returns Record mapping field keys to error messages
 */
export function validateRule(
  rule: RuleCreate,
  inProgressKeyword?: string
): Record<string, string> {
  const errors: Record<string, string> = {};

  if (!rule.ruleName.trim()) {
    errors.name = 'Rule name is required.';
  }

  if (
    rule.scope.level === 'FEED_SPECIFIC' &&
    (!rule.scope.targetFeeds || rule.scope.targetFeeds.length === 0)
  ) {
    errors.feeds =
      'At least one target feed must be selected for FEED_SPECIFIC scope.';
  }

  if (rule.conditions.evaluationType === 'KEYWORD_MATCH') {
    const activeKeywords = [...rule.conditions.keywords];
    const trimmedInProgress = inProgressKeyword?.trim();
    if (trimmedInProgress) {
      const tempWords = trimmedInProgress
        .split(',')
        .map((w) => w.trim())
        .filter((w) => w.length > 0);
      for (const tw of tempWords) {
        if (!activeKeywords.includes(tw)) activeKeywords.push(tw);
      }
    }

    if (activeKeywords.length === 0) {
      errors.keywords =
        'At least one keyword is required for Keyword Match rules.';
    }
  } else if (rule.conditions.evaluationType === 'REGEX_MATCH') {
    const expression = rule.conditions.expression;
    if (!expression.trim()) {
      errors.regexExpression = 'Regex expression is required.';
    }
    try {
      new RegExp(expression.trim());
    } catch (err) {
      errors.regexExpression = `Invalid regex expression: ${(err as Error).message}`;
    }
  } else if (rule.conditions.evaluationType === 'RULE_GROUP') {
    if (
      !rule.conditions.childRuleIds ||
      rule.conditions.childRuleIds.length === 0
    ) {
      errors.childRules =
        'At least one child rule must be selected for Rule Group.';
    }
  }

  return errors;
}

/**
 * Constructs a rule creation payload from the current form editing rule state,
 * integrating any in-progress typed keyword.
 *
 * @param editingRule The current rule state from the form fields
 * @param inProgressKeyword Optional keyword currently in the input field
 * @returns The constructed RuleCreate payload
 */
export function buildRulePayload(
  editingRule: RuleCreate,
  inProgressKeyword?: string
): RuleCreate {
  const finalKeywords =
    editingRule.conditions.evaluationType === 'KEYWORD_MATCH'
      ? [...editingRule.conditions.keywords]
      : [];

  const trimmedInProgress = inProgressKeyword?.trim();
  if (
    editingRule.conditions.evaluationType === 'KEYWORD_MATCH' &&
    trimmedInProgress
  ) {
    const tempWords = trimmedInProgress
      .split(',')
      .map((w) => w.trim())
      .filter((w) => w.length > 0);
    for (const w of tempWords) {
      if (!finalKeywords.includes(w)) {
        finalKeywords.push(w);
      }
    }
  }

  let conditionsPayload: RuleConditions;
  switch (editingRule.conditions.evaluationType) {
    case 'KEYWORD_MATCH':
      conditionsPayload = {
        evaluationType: 'KEYWORD_MATCH',
        operator: editingRule.conditions.operator,
        keywords: finalKeywords,
        caseSensitive: editingRule.conditions.caseSensitive,
      };
      break;
    case 'REGEX_MATCH':
      conditionsPayload = {
        evaluationType: 'REGEX_MATCH',
        expression: editingRule.conditions.expression.trim(),
        flags: editingRule.conditions.flags.trim(),
      };
      break;
    case 'RULE_GROUP':
      conditionsPayload = {
        evaluationType: 'RULE_GROUP',
        operator: editingRule.conditions.operator,
        childRuleIds: editingRule.conditions.childRuleIds,
      };
      break;
  }

  const scopePayload = {
    level: editingRule.scope.level,
    targetFeeds:
      editingRule.scope.level === 'GLOBAL' ? [] : editingRule.scope.targetFeeds,
  };

  return {
    ruleName: editingRule.ruleName.trim(),
    description: editingRule.description?.trim() || undefined,
    isActive: editingRule.isActive,
    scope: scopePayload,
    conditions: conditionsPayload,
  };
}
