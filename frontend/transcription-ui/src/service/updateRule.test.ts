import { beforeEach, describe, expect, it, vi } from 'vitest';

import { updateRule } from './updateRule';

describe('updateRule', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should update rule successfully', async () => {
    const ruleUpdate = {
      ruleName: 'Updated Terms',
      isActive: false,
    };

    const mockResponseData = {
      ruleId: 'rule-123',
      ruleName: 'Updated Terms',
      isActive: false,
      scope: {
        level: 'FEED' as const,
        targetFeeds: ['feed-1'],
      },
      conditions: {
        evaluationType: 'KEYWORD_MATCH' as const,
        operator: 'OR' as const,
        keywords: ['fire', 'evacuate'],
        caseSensitive: false,
      },
      metadata: {
        createdAt: '2026-06-03T00:00:00Z',
        updatedAt: '2026-06-03T01:00:00Z',
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

    const rule = await updateRule('rule-123', ruleUpdate, 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/rules/rule-123'),
      expect.objectContaining({
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer tokenXYZ',
        },
        body: JSON.stringify(ruleUpdate),
      })
    );
    expect(rule).toEqual(mockResponseData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
      headers: {
        get: () => null,
      },
      text: async () => 'Not Found',
    });

    await expect(
      updateRule('rule-not-found', { ruleName: 'New' }, 'tokenXYZ')
    ).rejects.toThrow(/404.*Not Found/);
  });
});
