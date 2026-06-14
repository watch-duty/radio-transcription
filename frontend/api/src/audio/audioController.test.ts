import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AudioController } from './audioController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  AUDIO_SEGMENTS_API_URL: 'http://audio-segments.example.com/v1/audio_segments',
}));

// Variables prefixed with 'mock' can be used in vi.mock
const mockRequest = vi.fn();

vi.mock('google-auth-library', () => {
  class MockGoogleAuth {
    getIdTokenClient = vi.fn().mockResolvedValue({
      request: mockRequest,
    });
  }
  return {
    GoogleAuth: MockGoogleAuth,
  };
});

describe('listAudioSegments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted data on success', async () => {
    const mockBackendResponse = {
      segments: [
        {
          id: 'segment-1',
          feed_id: 'feed-1',
          classification: 'SPEECH',
          start_timestamp: '2026-01-01T10:00:00Z',
          end_timestamp: '2026-01-01T10:01:00Z',
          missing_prior_context: false,
          missing_post_context: false,
          source_audio_uris: ['gs://bucket/audio.ogg'],
          canonical_audio_uri: 'gs://bucket/canonical.ogg',
          start_audio_offset: 'PT5S',
          end_audio_offset: 'PT10S',
          playback_audio_uri: 'https://example.com/playback.mp3',
          external_audio_segment_id: 'test-external-id',
          created_at: '2026-01-01T10:02:00Z',
          annotations: [
            {
              audio_segment_id: 'segment-1',
              type: 'TRANSCRIPT',
              created_at: '2026-01-01T10:03:00Z',
              data: {
                text: 'Hello world',
                errors: [],
              },
            },
          ],
        },
      ],
      next_token: 'next-page-token',
    };

    const expectedResult = {
      segments: [
        {
          id: 'segment-1',
          feedId: 'feed-1',
          classification: 'SPEECH',
          startTimestamp: '2026-01-01T10:00:00Z',
          endTimestamp: '2026-01-01T10:01:00Z',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: ['gs://bucket/audio.ogg'],
          canonicalAudioUri: 'gs://bucket/canonical.ogg',
          startAudioOffset: 'PT5S',
          endAudioOffset: 'PT10S',
          playbackAudioUri: 'https://example.com/playback.mp3',
          externalAudioSegmentId: 'test-external-id',
          createdAt: '2026-01-01T10:02:00Z',
          annotations: [
            {
              type: 'TRANSCRIPT',
              createdAt: '2026-01-01T10:03:00Z',
              data: {
                text: 'Hello world',
                errors: [],
              },
            },
          ],
        },
      ],
      nextToken: 'next-page-token',
    };

    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    const result = await controller.listAudioSegments('test', {
      limit: 100,
      isAlert: false,
    });

    expect(result).toEqual(expectedResult);
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=100&is_alert=false',
      method: 'GET',
    });
  });

  it('should forward is_alert parameter if true', async () => {
    const mockBackendResponse = { segments: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments('test', {
      limit: 100,
      isAlert: true,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=100&is_alert=true',
      method: 'GET',
    });
  });

  it('should forward is_alert parameter if false', async () => {
    const mockBackendResponse = { segments: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments('test', {
      limit: 100,
      isAlert: false,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=100&is_alert=false',
      method: 'GET',
    });
  });

  it('should not forward is_alert parameter if undefined', async () => {
    const mockBackendResponse = { segments: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments('test', {
      limit: 100,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=100',
      method: 'GET',
    });
  });

  it('should forward other query parameters if provided', async () => {
    const mockBackendResponse = { segments: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments('test', {
      limit: 50,
      nextToken: 'next-page-token',
      startTime: '2026-01-01T10:00:00Z',
      endTime: '2026-01-01T11:00:00Z',
      order: 'asc',
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=50&next_token=next-page-token&start_time=2026-01-01T10%3A00%3A00Z&end_time=2026-01-01T11%3A00%3A00Z&order=asc',
      method: 'GET',
    });
  });

  it('should forward repeated ids for batch hydration', async () => {
    const mockBackendResponse = { segments: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments('test', {
      limit: 100,
      ids: ['seg-1', 'seg-2'],
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments?feed_ids=test&limit=100&ids=seg-1&ids=seg-2',
      method: 'GET',
    });
  });

  it('should throw error on API failure with error message', async () => {
    const errorMessage = 'Backend Connection Failed';
    mockRequest.mockRejectedValueOnce(new Error(errorMessage));
    const controller = new AudioController();

    await expect(
      controller.listAudioSegments('test', { limit: 100, isAlert: false })
    ).rejects.toThrow(/Backend Connection Failed/);
  });
});

describe('getAudioSegment', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should fetch and convert a single segment by id', async () => {
    const mockBackendResponse = {
      id: 'segment-1',
      feed_id: 'feed-1',
      classification: 'SPEECH',
      start_timestamp: '2026-01-01T10:00:00Z',
      end_timestamp: '2026-01-01T10:01:00Z',
      missing_prior_context: false,
      missing_post_context: false,
      source_audio_uris: ['gs://bucket/audio.ogg'],
      canonical_audio_uri: 'gs://bucket/canonical.ogg',
      start_audio_offset: 'PT5S',
      end_audio_offset: 'PT10S',
      playback_audio_uri: 'https://example.com/playback.mp3',
      external_audio_segment_id: 'test-external-id',
      created_at: '2026-01-01T10:02:00Z',
      annotations: [],
    };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    const result = await controller.getAudioSegment('feed-1', 'segment-1');

    expect(result.id).toBe('segment-1');
    expect(result.canonicalAudioUri).toBe('gs://bucket/canonical.ogg');
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segments/segment-1',
      method: 'GET',
    });
  });

  it('should throw error on API failure', async () => {
    mockRequest.mockRejectedValueOnce(new Error('Not Found'));
    const controller = new AudioController();

    await expect(
      controller.getAudioSegment('feed-1', 'missing')
    ).rejects.toThrow(/Not Found/);
  });
});

describe('listAudioSegmentSummaries', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should convert summaries and default truncated to false', async () => {
    const mockBackendResponse = {
      segments: [
        {
          id: 'segment-1',
          feed_id: 'feed-1',
          start_timestamp: '2026-01-01T10:00:00Z',
          end_timestamp: '2026-01-01T10:01:00Z',
          classification: 'SPEECH',
          is_alert: true,
        },
      ],
    };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    const result = await controller.listAudioSegmentSummaries('test', {
      startTime: '2026-01-01T00:00:00Z',
    });

    expect(result).toEqual({
      segments: [
        {
          id: 'segment-1',
          feedId: 'feed-1',
          classification: 'SPEECH',
          startTimestamp: '2026-01-01T10:00:00Z',
          endTimestamp: '2026-01-01T10:01:00Z',
          isAlert: true,
        },
      ],
      truncated: false,
    });
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segment_summaries?feed_ids=test&start_time=2026-01-01T00%3A00%3A00Z',
      method: 'GET',
    });
  });

  it('should set truncated to the cursor and forward optional params', async () => {
    const mockBackendResponse = {
      segments: [],
      truncated: 'cursor-123',
    };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    const result = await controller.listAudioSegmentSummaries('test', {
      startTime: '2026-01-01T00:00:00Z',
      endTime: '2026-01-02T00:00:00Z',
      isAlert: false,
      nextToken: 'prev-cursor',
    });

    expect(result.truncated).toBe('cursor-123');
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com/v1/audio_segment_summaries?feed_ids=test&start_time=2026-01-01T00%3A00%3A00Z&end_time=2026-01-02T00%3A00%3A00Z&is_alert=false&next_token=prev-cursor',
      method: 'GET',
    });
  });

  it('should throw error on API failure', async () => {
    mockRequest.mockRejectedValueOnce(new Error('Backend Connection Failed'));
    const controller = new AudioController();

    await expect(
      controller.listAudioSegmentSummaries('test', {
        startTime: '2026-01-01T00:00:00Z',
      })
    ).rejects.toThrow(/Backend Connection Failed/);
  });
});
