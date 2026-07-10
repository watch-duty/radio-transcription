import { describe, expect, it, vi } from 'vitest';

import { toCamel } from './utils.js';

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
});
