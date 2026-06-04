import { describe, expect, it } from 'vitest';

import type { RuleCreate } from '@transcription/common';

import { buildRulePayload, validateRule } from './validationUtils';

describe('validateRule', () => {
  it('validates rule name is required', () => {
    const rule: RuleCreate = {
      ruleName: '   ',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule);
    expect(errors.name).toBe('Rule name is required.');
  });

  it('validates FEED_SPECIFIC scope requires target feeds', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'FEED_SPECIFIC', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule);
    expect(errors.feeds).toBe(
      'At least one target feed must be selected for FEED_SPECIFIC scope.'
    );
  });

  it('passes validation with target feeds in FEED_SPECIFIC scope', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'FEED_SPECIFIC', targetFeeds: ['feed-1'] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule);
    expect(errors.feeds).toBeUndefined();
  });

  it('validates keyword match requires keywords', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: [],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule);
    expect(errors.keywords).toBe(
      'At least one keyword is required for Keyword Match rules.'
    );
  });

  it('allows keyword match to use in-progress keywords', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: [],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule, 'dispatch');
    expect(errors.keywords).toBeUndefined();
  });

  it('validates regex expression is required and valid', () => {
    const rule1: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '   ',
        flags: '',
      },
    };

    const errors1 = validateRule(rule1);
    expect(errors1.regexExpression).toBe('Regex expression is required.');

    const rule2: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '[invalid',
        flags: '',
      },
    };

    const errors2 = validateRule(rule2);
    expect(errors2.regexExpression).toContain('Invalid regex expression:');
  });

  it('validates rule group requires child rule ids', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'RULE_GROUP',
        operator: 'ANY',
        childRuleIds: [],
      },
    };

    const errors = validateRule(rule);
    expect(errors.childRules).toBe(
      'At least one child rule must be selected for Rule Group.'
    );
  });

  it('passes validation for valid rule config', () => {
    const rule: RuleCreate = {
      ruleName: 'Valid Name',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const errors = validateRule(rule);
    expect(Object.keys(errors).length).toBe(0);
  });
});

describe('buildRulePayload', () => {
  it('constructs a rule payload correctly trimming name and description', () => {
    const input: RuleCreate = {
      ruleName: '  Alert Rule  ',
      description: '  Dispatch triggers  ',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const result = buildRulePayload(input);
    expect(result.ruleName).toBe('Alert Rule');
    expect(result.description).toBe('Dispatch triggers');
  });

  it('incorporates in-progress keyword and filters duplicates', () => {
    const input: RuleCreate = {
      ruleName: 'Alert Rule',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire'],
        caseSensitive: false,
      },
    };

    const result = buildRulePayload(input, ' evacuation, fire, alarm ');
    expect(result.conditions.evaluationType).toBe('KEYWORD_MATCH');
    if (result.conditions.evaluationType === 'KEYWORD_MATCH') {
      expect(result.conditions.keywords).toEqual([
        'fire',
        'evacuation',
        'alarm',
      ]);
    }
  });

  it('clears targetFeeds when scope level is GLOBAL', () => {
    const input: RuleCreate = {
      ruleName: 'Alert Rule',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: ['feed-1', 'feed-2'] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '.*',
        flags: 'i',
      },
    };

    const result = buildRulePayload(input);
    expect(result.scope.level).toBe('GLOBAL');
    expect(result.scope.targetFeeds).toEqual([]);
  });

  it('keeps targetFeeds when scope level is FEED_SPECIFIC', () => {
    const input: RuleCreate = {
      ruleName: 'Alert Rule',
      isActive: true,
      scope: { level: 'FEED_SPECIFIC', targetFeeds: ['feed-1'] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '.*',
        flags: 'i',
      },
    };

    const result = buildRulePayload(input);
    expect(result.scope.level).toBe('FEED_SPECIFIC');
    expect(result.scope.targetFeeds).toEqual(['feed-1']);
  });
});
