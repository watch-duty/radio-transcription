import { describe, expect, it } from 'vitest';

import { groupTagsByKey } from './tagDisplay';

describe('groupTagsByKey', () => {
  it('collapses repeated keys into one group, preserving order', () => {
    const tags = [
      { key: 'region', value: 'adams' },
      { key: 'county', value: 'Marin' },
      { key: 'region', value: 'coco' },
    ];
    expect(groupTagsByKey(tags)).toEqual([
      { key: 'region', values: ['adams', 'coco'] },
      { key: 'county', values: ['Marin'] },
    ]);
  });

  it('returns an empty array for no tags', () => {
    expect(groupTagsByKey([])).toEqual([]);
  });

  it('keeps single-value keys as one-element groups', () => {
    expect(groupTagsByKey([{ key: 'region', value: 'adams' }])).toEqual([
      { key: 'region', values: ['adams'] },
    ]);
  });
});
