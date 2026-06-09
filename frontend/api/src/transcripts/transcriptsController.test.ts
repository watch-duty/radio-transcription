import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TranscriptsController } from './transcriptsController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  TRANSCRIPTS_API_URL: 'http://api.example.com',
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

describe('listTranscripts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted data on success', async () => {
    const mockBackendResponse = {
      transcripts: [
        {
          feed_id: 'test',
          segment_id: '1',
          transcript: 'hello',
          start_timestamp: '1',
          end_timestamp: '2',
          missing_prior_context: false,
          missing_post_context: false,
          source_audio_uris: [],
          canonical_audio_uri: '',
          playback_audio_uri: '',
          start_audio_offset: '0',
          end_audio_offset: '0',
          evaluation_decisions: ['rule-1'],
          rule_annotations: {
            'rule-1': {
              text_match: [{ start: 0, end: 5, matched_text: 'hello' }],
            },
          },
        },
      ],
    };

    const expectedResult = {
      transcripts: [
        {
          feedId: 'test',
          segmentId: '1',
          transcript: 'hello',
          startTimestamp: '1',
          endTimestamp: '2',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: [],
          canonicalAudioUri: '',
          playbackAudioUri: '',
          startAudioOffset: '0',
          endAudioOffset: '0',
          evaluationDecisions: ['rule-1'],
          ruleAnnotations: {
            'rule-1': {
              textMatch: [{ startIndex: 0, endIndex: 5, matchedText: 'hello' }],
            },
          },
        },
      ],
    };

    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new TranscriptsController();
    const result = await controller.listTranscripts('test', {
      limit: 100,
    });

    expect(result).toEqual(expectedResult);
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://api.example.com?feed_id=test&limit=100',
      method: 'GET',
    });
  });

  it('should forward is_alert parameter if provided', async () => {
    const mockBackendResponse = { transcripts: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new TranscriptsController();
    await controller.listTranscripts('test', {
      limit: 100,
      isAlert: true,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://api.example.com?feed_id=test&limit=100&is_alert=true',
      method: 'GET',
    });
  });

  it('should forward is_alert parameter if false', async () => {
    const mockBackendResponse = { transcripts: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new TranscriptsController();
    await controller.listTranscripts('test', {
      limit: 100,
      isAlert: false,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://api.example.com?feed_id=test&limit=100&is_alert=false',
      method: 'GET',
    });
  });

  it('should not forward is_alert parameter if undefined', async () => {
    const mockBackendResponse = { transcripts: [] };
    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new TranscriptsController();
    await controller.listTranscripts('test', {
      limit: 100,
    });

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://api.example.com?feed_id=test&limit=100',
      method: 'GET',
    });
  });

  it('should throw error on API failure with error message', async () => {
    const errorMessage = 'Network Error';
    mockRequest.mockRejectedValueOnce(new Error(errorMessage));
    const controller = new TranscriptsController();

    await expect(
      controller.listTranscripts('test', { limit: 100 })
    ).rejects.toThrow(/Network Error/);
  });
});
