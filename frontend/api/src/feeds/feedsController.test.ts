import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FeedsController } from './feedsController.js';

// Mock the config module
vi.mock('../config.js', () => ({
  FEEDS_STORE_API_URL: 'http://feeds-api.example.com',
}));

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

describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const mockBackendFeed = {
    id: 'feed_123',
    name: 'Test Feed',
    source_type: 'openmhz',
    source_feed_id: 'src_123',
    external_id: 'ext_123',
  };

  const expectedFrontendFeed = {
    id: 'feed_123',
    name: 'Test Feed',
    sourceType: 'openmhz',
    sourceFeedId: 'src_123',
    externalId: 'ext_123',
  };

  describe('listFeeds', () => {
    it('should return converted feeds on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });

      const controller = new FeedsController();
      const result = await controller.listFeeds();

      expect(result).toEqual([expectedFrontendFeed]);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com',
        method: 'GET',
      });
    });

    it('should throw error on API failure', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));
      const controller = new FeedsController();

      await expect(controller.listFeeds()).rejects.toThrow(
        'Error fetching feeds'
      );
    });
  });

  describe('getFeed', () => {
    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.getFeed('feed_123', vi.fn());

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'GET',
      });
    });

    it('should call notFound on 404', async () => {
      const mockNotFound = vi.fn();
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await controller.getFeed('feed_123', mockNotFound);

      expect(mockNotFound).toHaveBeenCalledWith(404, {
        message: 'Feed feed_123 not found',
      });
    });
  });

  describe('createFeed', () => {
    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const payload = {
        name: 'Test Feed',
        sourceType: 'openmhz' as const,
        sourceFeedId: 'src_123',
        externalId: 'ext_123',
      };
      const result = await controller.createFeed(payload);

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com',
        method: 'POST',
        data: {
          name: 'Test Feed',
          source_type: 'openmhz',
          source_feed_id: 'src_123',
          external_id: 'ext_123',
        },
      });
    });
  });

  describe('deleteFeed', () => {
    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deleteFeed('feed_123', vi.fn());

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'DELETE',
      });
    });

    it('should call notFound on 404', async () => {
      const mockNotFound = vi.fn();
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await controller.deleteFeed('feed_123', mockNotFound);

      expect(mockNotFound).toHaveBeenCalledWith(404, {
        message: 'Feed feed_123 not found',
      });
    });
  });
});
