import { SourceType } from '@transcription/common';
import type { RuleConditions, RuleCreate, Tag } from '@transcription/common';

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
  inProgressKeyword?: string,
  tags?: Tag[]
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
    tags: tags ?? editingRule.tags ?? [],
  };
}

/**
 * The per-key policy the tag validators need. Callers define a superset (with
 * UI-only fields like display labels) and should `extends TagKeyLimit` so the
 * shared field names can't silently drift.
 */
export interface TagKeyLimit {
  maxValues?: number; // max times the key may appear (omit → unlimited)
  options?: readonly string[]; // values offered in the dropdown (omit → free-text)
  // Custom value check; takes precedence over `options` membership. Lets a key
  // accept more than the dropdown offers (e.g. non-canonical timezone aliases).
  validate?: (value: string) => boolean;
}

type TagKeyLimits = Record<string, TagKeyLimit>;

function maxValuesMessage(key: string, maxValues: number): string {
  return `The key "${key}" allows at most ${maxValues} value${maxValues === 1 ? '' : 's'}.`;
}

/**
 * Counts how many tags share a key. Keys are trimmed so the count stays
 * consistent with the (trimmed) config lookup used to resolve limits.
 */
export function countTagsWithKey(tags: Tag[], key: string): number {
  const trimmedKey = key.trim();
  return tags.filter((t) => t.key.trim() === trimmedKey).length;
}

/**
 * Returns an error message if a `{ key, value }` tag cannot be added to `tags` —
 * because it would exceed the key's `maxValues` limit, or exactly duplicates an
 * existing tag — or null if it can be added. A key may repeat with different
 * values; only an exact repeat is rejected. Inputs are trimmed before comparison.
 */
export function tagAddError(
  tags: Tag[],
  key: string,
  value: string,
  maxValues?: number
): string | null {
  const trimmedKey = key.trim();
  const trimmedValue = value.trim();
  if (maxValues != null && countTagsWithKey(tags, trimmedKey) >= maxValues) {
    return maxValuesMessage(trimmedKey, maxValues);
  }
  if (tags.some((t) => t.key === trimmedKey && t.value === trimmedValue)) {
    return `The tag "${trimmedKey}=${trimmedValue}" already exists.`;
  }
  return null;
}

/**
 * Validates a feed's tags, folding in any in-progress tag from the input row.
 * Returns a single error message string, or null when valid.
 */
export function validateTags(
  tags: Tag[],
  inProgressTag: { key: string; value: string },
  keyConfig: TagKeyLimits
): string | null {
  const configFor = (key: string) => keyConfig[key.trim()];
  const combined = [...tags];

  const trimmedKey = inProgressTag.key.trim();
  const trimmedValue = inProgressTag.value.trim();
  if (trimmedKey && trimmedValue) {
    const addError = tagAddError(
      tags,
      trimmedKey,
      trimmedValue,
      configFor(trimmedKey)?.maxValues
    );
    if (addError) return addError;
    combined.push({ key: trimmedKey, value: trimmedValue });
  } else if (trimmedKey || trimmedValue) {
    return 'Both key and value must be populated to add a tag.';
  }

  for (const tag of combined) {
    const config = configFor(tag.key);
    if (!config) continue;
    const value = tag.value.trim();
    const valid = config.validate
      ? config.validate(value)
      : !config.options || config.options.includes(value);
    if (!valid) {
      return `Invalid value for "${tag.key.trim()}". Please select a valid option from the list.`;
    }
  }

  const overLimitKey = [...new Set(combined.map((t) => t.key))].find((key) => {
    const limit = configFor(key)?.maxValues;
    return limit != null && countTagsWithKey(combined, key) > limit;
  });
  if (overLimitKey) {
    return maxValuesMessage(overLimitKey, configFor(overLimitKey)!.maxValues!);
  }

  if (combined.some((tag) => !tag.key.trim() || !tag.value.trim())) {
    return 'Tag key and value inputs cannot be blank. Discard empty tag rows using the delete button.';
  }

  return null;
}
