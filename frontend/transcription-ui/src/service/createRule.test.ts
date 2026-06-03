import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { RuleCreate } from '@transcription/common';

import { createRule } from './createRule';

describe('createRule', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should create rule successfully', async () => {
    const ruleCreate: RuleCreate = {
      ruleName: 'Critical Terms',
      isActive: true,
      scope: {
        level: 'FEED_SPECIFIC',
        targetFeeds: ['feed-1'],
      },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['fire', 'evacuate'],
        caseSensitive: false,
      },
    };

    const mockResponseData = {
      ruleId: 'rule-123',
      ...ruleCreate,
      metadata: {
        createdAt: '2026-06-03T00:00:00Z',
        updatedAt: '2026-06-03T00:00:00Z',
      },
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockResponseData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const rule = await createRule(ruleCreate, 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/rules'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer tokenXYZ',
        },
        body: JSON.stringify(ruleCreate),
      })
    );
    expect(rule).toEqual(mockResponseData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      headers: {
        get: () => null,
      },
      text: async () => 'Invalid payload',
    });

    await expect(
      createRule(
        {
          ruleName: 'Critical Terms',
          isActive: true,
          scope: {
            level: 'GLOBAL',
            targetFeeds: [],
          },
          conditions: {
            evaluationType: 'KEYWORD_MATCH',
            operator: 'ANY',
            keywords: ['bad'],
            caseSensitive: true,
          },
        },
        'tokenXYZ'
      )
    ).rejects.toThrow(/400.*Bad Request/);
  });
});
