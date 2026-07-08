import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listFeedHistory } from './listFeedHistory';

describe('listFeedHistory', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch feed history successfully', async () => {
    const mockData = {
      historyEvents: [
        {
          id: 'event-1',
          feedId: 'feed-123',
          action: 'update',
          actor: 'user-1',
          occurredAt: Date.parse('2026-07-07T12:00:00Z'),
          feedRevision: 1,
          beforeValues: { status: 'inactive' },
          afterValues: { status: 'active' },
        },
      ],
      total: 1,
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const response = await listFeedHistory('feed-123', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/feed-123/history'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(response).toEqual(mockData);
  });

  it('should fetch feed history with limit and nextToken', async () => {
    const mockData = {
      historyEvents: [],
      total: 0,
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    await listFeedHistory('feed-123', 'tokenXYZ', 50, 'token123');

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(
        '/api/v1/feeds/feed-123/history?limit=50&nextToken=token123'
      ),
      expect.any(Object)
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

    await expect(listFeedHistory('feed-123', 'tokenXYZ')).rejects.toThrow(
      /404.*Not Found/
    );
  });
});
