import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listAudioSegments } from './listAudioSegments';

describe('listAudioSegments', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch audio segments successfully', async () => {
    const mockData = {
      segments: [
        {
          id: 'segment-1',
          feedId: 'feed123',
          classification: 'SPEECH',
          startTimestamp: '2026-04-10T12:00:00Z',
          endTimestamp: '2026-04-10T12:01:00Z',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: [],
          createdAt: '2026-04-10T12:02:00Z',
        },
      ],
      nextToken: 'next-page-token',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const result = await listAudioSegments(
      'feed123',
      'tokenXYZ',
      50,
      'next-page-token',
      '2026-04-10T12:00:00Z',
      '2026-04-10T12:05:00Z',
      'desc',
      false
    );

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(
        '/api/v1/audioSegments/feed123?limit=50&nextToken=next-page-token&startTime=2026-04-10T12%3A00%3A00Z&endTime=2026-04-10T12%3A05%3A00Z&order=desc&isAlert=false'
      ),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(result).toEqual({
      segments: mockData.segments,
      nextToken: 'next-page-token',
    });
  });

  it('should include isAlert in query parameter when specified', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify({ segments: [] }),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    await listAudioSegments(
      'feed123',
      'tokenXYZ',
      100,
      undefined,
      undefined,
      undefined,
      undefined,
      true
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(
        '/api/v1/audioSegments/feed123?limit=100&isAlert=true'
      ),
      expect.any(Object)
    );
  });

  it('should return empty array if segments missing in response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify({}),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const result = await listAudioSegments('feed123', 'tokenXYZ', 100);

    expect(result).toEqual({ segments: [], nextToken: undefined });
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      headers: {
        get: () => null,
      },
      text: async () => 'Forbidden',
    });

    await expect(listAudioSegments('feed123', 'tokenXYZ', 100)).rejects.toThrow(
      /403.*Forbidden/
    );
  });
});
