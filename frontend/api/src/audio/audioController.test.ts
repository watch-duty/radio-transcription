import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AudioController } from './audioController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  AUDIO_API_URL: 'http://audio-segments.example.com',
}));

// Variables prefixed with 'mock' can be used in vi.mock
const mockRequest = vi.fn();

vi.mock('google-auth-library', () => {
  return {
    GoogleAuth: vi.fn().mockImplementation(() => ({
      getIdTokenClient: vi.fn().mockResolvedValue({
        request: mockRequest,
      }),
    })),
  };
});

describe('listAudioSegments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted data on success', async () => {
    const mockBackendResponse = [
      {
        id: 'segment-1',
        feed_id: 'feed-1',
        classification: 'SPEECH_DETECTED',
        start_timestamp: '2026-01-01T10:00:00Z',
        end_timestamp: '2026-01-01T10:01:00Z',
        missing_prior_context: false,
        missing_post_context: false,
        source_audio_uris: ['gs://bucket/audio.ogg'],
        canonical_audio_uri: 'gs://bucket/canonical.ogg',
        start_audio_offset: 'PT5S',
        end_audio_offset: 'PT10S',
        playback_audio_uri: 'https://example.com/playback.mp3',
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
    ];

    const expectedResult = [
      {
        id: 'segment-1',
        feedId: 'feed-1',
        classification: 'SPEECH_DETECTED',
        startTimestamp: '2026-01-01T10:00:00Z',
        endTimestamp: '2026-01-01T10:01:00Z',
        missingPriorContext: false,
        missingPostContext: false,
        sourceAudioUris: ['gs://bucket/audio.ogg'],
        canonicalAudioUri: 'gs://bucket/canonical.ogg',
        startAudioOffset: 'PT5S',
        endAudioOffset: 'PT10S',
        playbackAudioUri: 'https://example.com/playback.mp3',
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
    ];

    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    const result = await controller.listAudioSegments({});

    expect(result).toEqual(expectedResult);
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com?',
      method: 'GET',
    });
  });

  it('should forward feedIds query parameters if provided', async () => {
    const mockBackendResponse: unknown[] = [];
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new AudioController();
    await controller.listAudioSegments({
      feedIds: ['feed-1', 'feed-2'],
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://audio-segments.example.com?feed_ids=feed-1&feed_ids=feed-2',
      method: 'GET',
    });
  });

  it('should throw error on API failure with error message', async () => {
    const errorMessage = 'Backend Connection Failed';
    mockRequest.mockRejectedValueOnce(new Error(errorMessage));
    const controller = new AudioController();

    await expect(controller.listAudioSegments({})).rejects.toThrow(
      /Backend Connection Failed/
    );
  });
});
