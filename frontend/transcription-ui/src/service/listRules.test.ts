import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listRules } from './listRules';

describe('listRules', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch rules successfully', async () => {
    const mockData = [
      { ruleId: '1', ruleName: 'Feed Rules' },
      { ruleId: '2', ruleName: 'Critical Terms' },
    ];

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) => (key === 'content-type' ? 'application/json' : null),
      },
    });

    const rules = await listRules('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/rules'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(rules).toEqual(mockData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      headers: {
        get: () => null,
      },
      text: async () => 'Unauthorized',
    });

    await expect(listRules('tokenXYZ')).rejects.toThrow(/401.*Unauthorized/);
  });
});
