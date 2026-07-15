import { describe, expect, it, vi } from 'vitest';

import { parseTimestamp, toCamel, toSnake } from './utils.js';

vi.mock('./config.js', () => ({
  AUTH_BACKEND: 'none',
}));

describe('toCamel', () => {
  it('converts snake_case keys to camelCase', () => {
    const input = {
      source_type: 'foo',
      failure_count: 5,
      status_reason_detail: 'bar',
    };
    const expected = {
      sourceType: 'foo',
      failureCount: 5,
      statusReasonDetail: 'bar',
    };
    expect(toCamel(input)).toEqual(expected);
  });

  it('handles nested objects', () => {
    const input = {
      outer_key: {
        inner_key: 'value',
      },
    };
    const expected = {
      outerKey: {
        innerKey: 'value',
      },
    };
    expect(toCamel(input)).toEqual(expected);
  });

  it('handles arrays of objects', () => {
    const input = {
      my_list: [{ item_one: 1 }, { item_two: 2 }],
    };
    const expected = {
      myList: [{ itemOne: 1 }, { itemTwo: 2 }],
    };
    expect(toCamel(input)).toEqual(expected);
  });

  it('handles null and primitive values', () => {
    expect(toCamel(null)).toBeNull();
    expect(toCamel('string')).toEqual('string');
    expect(toCamel(123)).toEqual(123);
  });

  it('handles arrays at root', () => {
    const input = [{ my_key: 'val' }];
    const expected = [{ myKey: 'val' }];
    expect(toCamel(input)).toEqual(expected);
  });

  it('converts null values to undefined when nullToUndefined is true', () => {
    const input = {
      valid_key: 'foo',
      nullable_key: null,
      nested_obj: {
        inner_null: null,
        inner_val: 42,
      },
    };
    const expected = {
      validKey: 'foo',
      nullableKey: undefined,
      nestedObj: {
        innerNull: undefined,
        innerVal: 42,
      },
    };
    expect(toCamel(input, { nullToUndefined: true })).toEqual(expected);
  });
});

describe('parseTimestamp', () => {
  it('converts a valid ISO date string to milliseconds', () => {
    const isoStr = '2026-06-26T12:34:56.000Z';
    expect(parseTimestamp(isoStr)).toEqual(Date.parse(isoStr));
  });

  it('returns undefined for undefined, null, or empty input', () => {
    expect(parseTimestamp(undefined)).toBeUndefined();
    expect(parseTimestamp(null)).toBeUndefined();
    expect(parseTimestamp('')).toBeUndefined();
  });

  it('returns undefined for invalid date string format', () => {
    expect(parseTimestamp('invalid-timestamp')).toBeUndefined();
  });
});

describe('toSnake', () => {
  it('converts camelCase keys to snake_case recursively', () => {
    const input = {
      ruleName: 'test rule',
      isActive: true,
      nestedScope: {
        targetFeeds: ['feed1', 'feed2'],
        levelOption: 'GLOBAL',
      },
    };
    const expected = {
      rule_name: 'test rule',
      is_active: true,
      nested_scope: {
        target_feeds: ['feed1', 'feed2'],
        level_option: 'GLOBAL',
      },
    };
    expect(toSnake(input)).toEqual(expected);
  });

  it('handles null, undefined, and primitive values', () => {
    expect(toSnake(null)).toBeNull();
    expect(toSnake(undefined)).toBeUndefined();
    expect(toSnake('string')).toEqual('string');
    expect(toSnake(123)).toEqual(123);
  });

  it('handles arrays at root', () => {
    const input = [{ myKey: 'val', secondVal: 42 }];
    const expected = [{ my_key: 'val', second_val: 42 }];
    expect(toSnake(input)).toEqual(expected);
  });

  it('skips undefined properties across objects', () => {
    const input = {
      ruleName: 'updated name',
      description: undefined,
      isActive: true,
      scope: undefined,
    };
    const expected = {
      rule_name: 'updated name',
      is_active: true,
    };
    expect(toSnake(input)).toEqual(expected);
  });
});
