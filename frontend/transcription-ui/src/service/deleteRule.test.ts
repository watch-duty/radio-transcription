import { beforeEach, describe, expect, it, vi } from 'vitest';

import { deleteRule } from './deleteRule';

describe('deleteRule', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should delete rule successfully', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => '',
      headers: {
        get: () => null,
      },
    });

    await deleteRule('rule-123', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/rules/rule-123'),
      expect.objectContaining({
        method: 'DELETE',
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
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

    await expect(deleteRule('rule-not-found', 'tokenXYZ')).rejects.toThrow(
      /404.*Not Found/
    );
  });
});
