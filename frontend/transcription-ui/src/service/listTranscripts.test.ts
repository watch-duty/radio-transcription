import { describe, it, expect, vi, beforeEach } from 'vitest';
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
        { transmissionId: '1', transcript: 'Hello', startTimestamp: '2026-04-10T12:00:00Z' },
      ],
    };
    
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => mockData,
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
    expect(transcripts).toEqual(mockData.transcripts);
  });

  it('should return empty array if transcripts missing in response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({}),
    });

    const transcripts = await listTranscripts('feed123', 'tokenXYZ');

    expect(transcripts).toEqual([]);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
    });

    await expect(listTranscripts('feed123', 'tokenXYZ')).rejects.toThrow(
      'Error: 403 Forbidden'
    );
  });
});
