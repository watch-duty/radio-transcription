import axios from 'axios';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TranscriptsController } from './transcriptsController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  TRANSCRIPTS_API_URL: 'http://api.example.com',
}));

vi.mock('axios');
vi.mock('google-auth-library', () => {
  return {
    GoogleAuth: vi.fn().mockImplementation(() => ({
      getIdTokenClient: vi.fn().mockResolvedValue({
        getRequestHeaders: vi.fn().mockResolvedValue({
          get: vi.fn().mockReturnValue('Bearer mock-token'),
        }),
      }),
    })),
  };
});

describe('listTranscripts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return data on success', async () => {
    const mockData = {
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
    vi.mocked(axios.get).mockResolvedValueOnce({ data: mockData });

    const controller = new TranscriptsController();
    const result = await controller.listTranscripts('test', vi.fn());

    expect(result).toEqual(mockData);
    expect(axios.get).toHaveBeenCalledWith('http://api.example.com', {
      params: { feedId: 'test' },
      headers: { Authorization: 'Bearer mock-token' },
    });
  });

  it('should throw error on API failure with error message', async () => {
    const errorMessage = 'Network Error';
    vi.mocked(axios.get).mockRejectedValueOnce(new Error(errorMessage));
    const controller = new TranscriptsController();

    await expect(
      controller.listTranscripts('test', vi.fn())
    ).rejects.toThrow('Error fetching transcript: Network Error');
  });
});
