import { describe, it, expect, vi, beforeEach } from 'vitest';
import { listTranscripts } from './listTranscripts.js';
import axios from 'axios';
import { Request, Response } from 'express';

vi.mock('axios');
vi.mock('google-auth-library', () => {
  return {
    GoogleAuth: vi.fn().mockImplementation(() => ({
      getIdTokenClient: vi.fn().mockResolvedValue({
        getRequestHeaders: vi.fn().mockResolvedValue({
          get: vi.fn().mockReturnValue('Bearer mock-token')
        })
      })
    }))
  };
});

describe('listTranscripts', () => {
  let req: Partial<Request>;
  let res: Partial<Response>;

  beforeEach(() => {
    req = { method: 'GET', body: { feedId: 'test' } };
    res = {
      status: vi.fn().mockReturnThis(),
      send: vi.fn(),
      json: vi.fn(),
    };
    vi.clearAllMocks();
  });

  it('should return 200 and data on success', async () => {
    process.env.TRANSCRIPT_API_URL = 'http://api.example.com';
    const mockData = { transcripts: [{ id: '1', title: 'test' }] };
    vi.mocked(axios.get).mockResolvedValueOnce({ data: mockData });

    await listTranscripts(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith(mockData);
    expect(axios.get).toHaveBeenCalledWith('http://api.example.com', {
      params: { feedId: 'test' },
      headers: { Authorization: 'Bearer mock-token' }
    });
  });

  it('should return 500 if TRANSCRIPT_API_URL is missing', async () => {
    delete process.env.TRANSCRIPT_API_URL;

    await listTranscripts(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.send).toHaveBeenCalledWith('TRANSCRIPT_API_URL environment variable is not set');
  });

  it('should return 500 on API failure with error message', async () => {
    process.env.TRANSCRIPT_API_URL = 'http://api.example.com';
    const errorMessage = 'Network Error';
    vi.mocked(axios.get).mockRejectedValueOnce(new Error(errorMessage));

    await listTranscripts(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.send).toHaveBeenCalledWith('Error fetching transcript: Network Error');
  });

  it('should return 405 for non-GET methods', async () => {
    req.method = 'POST';

    await listTranscripts(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(405);
    expect(res.send).toHaveBeenCalledWith('Method Not Allowed');
  });
});
