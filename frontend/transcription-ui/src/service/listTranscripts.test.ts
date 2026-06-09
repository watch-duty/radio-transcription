import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listTranscripts } from './listTranscripts';

describe('listTranscripts', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch transcripts successfully', async () => {
    const mockData = {
      transcripts: [
        {
          segmentId: '1',
          transcript: 'Hello',
          startTimestamp: '2026-04-10T12:00:00Z',
        },
      ],
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const transcripts = await listTranscripts('feed123', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/transcripts/feed123'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(transcripts).toEqual({
      transcripts: mockData.transcripts,
      nextToken: undefined,
    });
  });

  it('should include isAlert in query parameter when specified', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify({ transcripts: [] }),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    await listTranscripts(
      'feed123',
      'tokenXYZ',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      true
    );

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/transcripts/feed123?isAlert=true'),
      expect.any(Object)
    );
  });

  it('should return empty array if transcripts missing in response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify({}),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const transcripts = await listTranscripts('feed123', 'tokenXYZ');

    expect(transcripts).toEqual({ transcripts: [], nextToken: undefined });
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

    await expect(listTranscripts('feed123', 'tokenXYZ')).rejects.toThrow(
      /403.*Forbidden/
    );
  });
});
