import { describe, it, expect, vi, beforeEach } from 'vitest';
import { getTranscript } from './getTranscript.js';
import axios from 'axios';
import { Request, Response } from 'express';

vi.mock('axios');

describe('getTranscript', () => {
  let req: Partial<Request>;
  let res: Partial<Response>;

  beforeEach(() => {
    req = { method: 'GET' };
    res = {
      status: vi.fn().mockReturnThis(),
      send: vi.fn(),
      json: vi.fn(),
    };
    vi.clearAllMocks();
  });

  it('should return 200 and data on success', async () => {
    process.env.TRANSCRIPT_API_URL = 'http://api.example.com';
    const mockData = { transcript: 'test' };
    vi.mocked(axios.get).mockResolvedValueOnce({ data: mockData });

    await getTranscript(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith(mockData);
  });

  it('should return 500 if TRANSCRIPT_API_URL is missing', async () => {
    delete process.env.TRANSCRIPT_API_URL;

    await getTranscript(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.send).toHaveBeenCalledWith('TRANSCRIPT_API_URL environment variable is not set');
  });

  it('should return 500 on API failure with error message', async () => {
    process.env.TRANSCRIPT_API_URL = 'http://api.example.com';
    const errorMessage = 'Network Error';
    vi.mocked(axios.get).mockRejectedValueOnce(new Error(errorMessage));

    await getTranscript(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.send).toHaveBeenCalledWith('Error fetching transcript: Network Error');
  });

  it('should return 405 for non-GET methods', async () => {
    req.method = 'POST';

    await getTranscript(req as Request, res as Response);

    expect(res.status).toHaveBeenCalledWith(405);
    expect(res.send).toHaveBeenCalledWith('Method Not Allowed');
  });
});
