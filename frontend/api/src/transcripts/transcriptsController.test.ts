import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TranscriptsController } from './transcriptsController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  TRANSCRIPTS_API_URL: 'http://api.example.com',
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

describe('listTranscripts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return converted data on success', async () => {
    const mockBackendResponse = {
      transcripts: [
        {
          feed_id: 'test',
          transmission_id: '1',
          transcript: 'hello',
          start_timestamp: '1',
          end_timestamp: '2',
          missing_prior_context: false,
          missing_post_context: false,
          source_audio_uris: [],
          canonical_audio_uri: '',
          start_audio_offset: '0',
          end_audio_offset: '0',
          evaluation_decisions: [],
        },
      ],
    };

    const expectedResult = {
      transcripts: [
        {
          feedId: 'test',
          transmissionId: '1',
          transcript: 'hello',
          startTimestamp: '1',
          endTimestamp: '2',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: [],
          canonicalAudioUri: '',
          startAudioOffset: '0',
          endAudioOffset: '0',
          evaluationDecisions: [],
        },
      ],
    };

    mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

    const controller = new TranscriptsController();
    const result = await controller.listTranscripts('test', vi.fn(), {
      limit: 100,
    });

    expect(result).toEqual(expectedResult);
    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://api.example.com?feed_id=test',
      method: 'GET',
    });
  });

  it('should throw error on API failure with error message', async () => {
    const errorMessage = 'Network Error';
    mockRequest.mockRejectedValueOnce(new Error(errorMessage));
    const controller = new TranscriptsController();

    await expect(
      controller.listTranscripts('test', vi.fn(), { limit: 100 })
    ).rejects.toThrow('Error fetching transcript: Network Error');
  });
});
