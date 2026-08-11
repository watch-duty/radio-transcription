import { describe, expect, it } from 'vitest';

import { type RuleCreate, SourceType } from '@transcription/common';

import {
  buildRulePayload,
  countTagsWithKey,
  isValidTimezone,
  tagAddError,
  validateFeedSourceId,
  validateRule,
  validateTags,
} from './validationUtils';

const TAG_CONFIG = {
  'system/timezone': { maxValues: 1, options: ['UTC', 'America/Los_Angeles'] },
};

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

    describe('GENERIC_ICECAST validation', () => {
      it('returns null for http and https stream URLs', () => {
        expect(
          validateFeedSourceId(
            SourceType.GENERIC_ICECAST,
            'http://tonasket.duckdns.org/okco'
          )
        ).toBeNull();
        expect(
          validateFeedSourceId(
            SourceType.GENERIC_ICECAST,
            'https://stream.example.org:8000/mount'
          )
        ).toBeNull();
      });

      it('returns error for a bare Broadcastify feed ID', () => {
        expect(validateFeedSourceId(SourceType.GENERIC_ICECAST, '12345')).toBe(
          'Must be a stream URL starting with http:// or https://.'
        );
      });

      it('returns error for other invalid formats', () => {
        const expectedError =
          'Must be a stream URL starting with http:// or https://.';
        expect(
          validateFeedSourceId(SourceType.GENERIC_ICECAST, 'example.org/mount')
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(
            SourceType.GENERIC_ICECAST,
            'ftp://example.org/x'
          )
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(SourceType.GENERIC_ICECAST, 'http://')
        ).toBe(expectedError);
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

  it('includes explicitly provided tags in the payload', () => {
    const input: RuleCreate = {
      ruleName: 'Alert Rule',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '.*',
        flags: 'i',
      },
    };

    const result = buildRulePayload(input, undefined, [
      { key: 'geo_event_type', value: 'flooding' },
    ]);
    expect(result.tags).toEqual([{ key: 'geo_event_type', value: 'flooding' }]);
  });

  it("falls back to the rule's existing tags, then an empty list", () => {
    const withTags: RuleCreate = {
      ruleName: 'Alert Rule',
      isActive: true,
      scope: { level: 'GLOBAL', targetFeeds: [] },
      conditions: {
        evaluationType: 'REGEX_MATCH',
        expression: '.*',
        flags: 'i',
      },
      tags: [{ key: 'team', value: 'goo' }],
    };
    expect(buildRulePayload(withTags).tags).toEqual([
      { key: 'team', value: 'goo' },
    ]);
    expect(buildRulePayload({ ...withTags, tags: undefined }).tags).toEqual([]);
  });

  describe('timezone validation', () => {
    describe('isValidTimezone', () => {
      it('returns true for valid timezones', () => {
        expect(isValidTimezone('UTC')).toBe(true);
        expect(isValidTimezone('America/Los_Angeles')).toBe(true);
        expect(isValidTimezone('America/New_York')).toBe(true);
        expect(isValidTimezone('Europe/London')).toBe(true);
      });

      it('returns false for invalid timezones', () => {
        expect(isValidTimezone('invalid-timezone')).toBe(false);
        expect(isValidTimezone('')).toBe(false);
        expect(isValidTimezone('America/New_Yorkish')).toBe(false);
      });
    });
  });

  describe('countTagsWithKey', () => {
    it('counts only tags matching the key', () => {
      const tags = [
        { key: 'region', value: 'a' },
        { key: 'region', value: 'b' },
        { key: 'county', value: 'c' },
      ];
      expect(countTagsWithKey(tags, 'region')).toBe(2);
      expect(countTagsWithKey(tags, 'county')).toBe(1);
      expect(countTagsWithKey(tags, 'missing')).toBe(0);
    });

    it('trims keys on both sides so whitespace variants count together', () => {
      const tags = [
        { key: 'system/timezone', value: 'UTC' },
        { key: 'system/timezone ', value: 'America/Los_Angeles' },
      ];
      expect(countTagsWithKey(tags, 'system/timezone')).toBe(2);
    });
  });

  describe('tagAddError', () => {
    it('allows an unlimited key to repeat with different values', () => {
      const tags = [{ key: 'region', value: 'sonoma-CA_US' }];
      expect(tagAddError(tags, 'region', 'elbert-CO_US')).toBeNull();
    });

    it('rejects an exact key+value duplicate', () => {
      const tags = [{ key: 'region', value: 'sonoma-CA_US' }];
      expect(tagAddError(tags, 'region', 'sonoma-CA_US')).toBe(
        'The tag "region=sonoma-CA_US" already exists.'
      );
    });

    it('rejects exceeding a key maxValues limit', () => {
      const tags = [{ key: 'system/timezone', value: 'UTC' }];
      expect(
        tagAddError(tags, 'system/timezone', 'America/Los_Angeles', 1)
      ).toBe('The key "system/timezone" allows at most 1 value.');
    });

    it('pluralizes the limit message for higher caps', () => {
      const tags = [
        { key: 'region', value: 'a' },
        { key: 'region', value: 'b' },
      ];
      expect(tagAddError(tags, 'region', 'c', 2)).toBe(
        'The key "region" allows at most 2 values.'
      );
    });
  });

  describe('validateTags', () => {
    const empty = { key: '', value: '' };

    it('returns null for valid multi-value tags', () => {
      const tags = [
        { key: 'region', value: 'sonoma-CA_US' },
        { key: 'region', value: 'elbert-CO_US' },
      ];
      expect(validateTags(tags, empty, TAG_CONFIG)).toBeNull();
    });

    it('folds in and rejects an in-progress duplicate', () => {
      const tags = [{ key: 'region', value: 'sonoma-CA_US' }];
      expect(
        validateTags(tags, { key: 'region', value: 'sonoma-CA_US' }, TAG_CONFIG)
      ).toBe('The tag "region=sonoma-CA_US" already exists.');
    });

    it('errors when only one of key/value is filled', () => {
      expect(validateTags([], { key: 'region', value: '' }, TAG_CONFIG)).toBe(
        'Both key and value must be populated to add a tag.'
      );
    });

    it('rejects an out-of-range enum value', () => {
      const tags = [{ key: 'system/timezone', value: 'Mars/Phobos' }];
      expect(validateTags(tags, empty, TAG_CONFIG)).toBe(
        'Invalid value for "system/timezone". Please select a valid option from the list.'
      );
    });

    it('accepts a value via a custom validate() even if not in options', () => {
      const config = {
        'system/timezone': {
          maxValues: 1,
          options: ['UTC'],
          validate: (v: string) => v === 'US/Pacific' || v === 'UTC',
        },
      };
      const tags = [{ key: 'system/timezone', value: 'US/Pacific' }];
      expect(validateTags(tags, empty, config)).toBeNull();
    });

    it('flags a pre-existing set that exceeds a key limit', () => {
      const tags = [
        { key: 'system/timezone', value: 'UTC' },
        { key: 'system/timezone', value: 'America/Los_Angeles' },
      ];
      expect(validateTags(tags, empty, TAG_CONFIG)).toBe(
        'The key "system/timezone" allows at most 1 value.'
      );
    });

    it('enforces the limit even when a key has stray whitespace', () => {
      const tags = [
        { key: 'system/timezone', value: 'UTC' },
        { key: 'system/timezone ', value: 'America/Los_Angeles' },
      ];
      expect(validateTags(tags, empty, TAG_CONFIG)).toContain(
        'at most 1 value'
      );
    });
  });
});
