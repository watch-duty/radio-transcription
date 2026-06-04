import { describe, expect, it } from 'vitest';

import { type RuleCreate, SourceType } from '@transcription/common';

import {
  buildRulePayload,
  validateFeedSourceId,
  validateRule,
} from './validationUtils';

describe('validationUtils', () => {
  describe('validateFeedSourceId', () => {
    it('returns error when sourceId is empty or whitespace only', () => {
      expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '')).toBe(
        'Source feed ID is required.'
      );
      expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '   ')).toBe(
        'Source feed ID is required.'
      );
    });

    describe('BCFY_CALLS validation', () => {
      it('returns null for valid format (numbers separated by a dash)', () => {
        expect(
          validateFeedSourceId(SourceType.BCFY_CALLS, '123-456')
        ).toBeNull();
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '0-0')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError =
          'Must only contain numbers formatted as sid-talkgroup.';
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123-')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '-456')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, 'abc-def')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123a-456')).toBe(
          expectedError
        );
      });
    });

    describe('BCFY_FEEDS validation', () => {
      it('returns null for valid format (only numbers)', () => {
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '12345')).toBeNull();
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '0')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError = 'Must be a number.';
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '123a')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '123-456')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, 'abc')).toBe(
          expectedError
        );
      });
    });

    describe('ECHO validation', () => {
      it('returns null for valid format (letters, numbers, dashes, underscores)', () => {
        expect(
          validateFeedSourceId(SourceType.ECHO, 'feed-123_abc')
        ).toBeNull();
        expect(validateFeedSourceId(SourceType.ECHO, 'ECHO_FEED')).toBeNull();
        expect(validateFeedSourceId(SourceType.ECHO, 'simple')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError =
          'Must only contain letters, numbers, and the following special characters: - _';
        expect(validateFeedSourceId(SourceType.ECHO, 'feed.123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.ECHO, 'feed/123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.ECHO, 'feed@123')).toBe(
          expectedError
        );
      });
    });

    describe('FIRE_NOTIFICATIONS validation', () => {
      it('returns null for valid format (uppercase letters, numbers, slash, dash, parentheses, underscore)', () => {
        expect(
          validateFeedSourceId(
            SourceType.FIRE_NOTIFICATIONS,
            'FIRE/DEPT-1(A)_B'
          )
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FDNY')
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, '123')
        ).toBeNull();
      });

      it('returns error for invalid formats (e.g. lowercase letters or unsupported characters)', () => {
        const expectedError =
          'Must only contain uppercase letters, numbers, and the following special characters: / - ( )';
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'fire')
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FIRE.DEPT')
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FIRE@DEPT')
        ).toBe(expectedError);
      });
    });

    describe('OPENMHZ validation', () => {
      it('returns null for valid format (alphanumeric, including underscores)', () => {
        expect(
          validateFeedSourceId(SourceType.OPENMHZ, 'openmhz123')
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.OPENMHZ, 'open_mhz_456')
        ).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError = 'Must be alphanumeric.';
        expect(validateFeedSourceId(SourceType.OPENMHZ, 'open-mhz')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.OPENMHZ, 'open.mhz')).toBe(
          expectedError
        );
      });
    });
  });
});

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
