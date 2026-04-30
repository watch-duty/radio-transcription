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
    sourceUrl: 'https://openmhz.com/system/src_123',
    archiveUrl: undefined,
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

      await expect(controller.listFeeds()).rejects.toThrow(/Network Error/);
    });
  });

  describe('getFeed', () => {
    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.getFeed('feed_123');

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'GET',
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(controller.getFeed('feed_123')).rejects.toThrow(/Not Found/);
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

  describe('resetFeed', () => {
    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.resetFeed('feed_123');

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/reset',
        method: 'POST',
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(controller.resetFeed('feed_123')).rejects.toThrow(
        /Not Found/
      );
    });

    it('should throw on non-404 API error', async () => {
      const error = new Error('Server Error') as Error & {
        response?: { status: number; data?: unknown };
      };
      error.response = { status: 500, data: 'Internal Server Error' };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(controller.resetFeed('feed_123')).rejects.toThrow(
        /Server Error/
      );
    });

    it('should throw on unexpected error', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));

      const controller = new FeedsController();
      await expect(controller.resetFeed('feed_123')).rejects.toThrow(
        /Network Error/
      );
    });
  });

  describe('deleteFeed', () => {
    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deleteFeed('feed_123');

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'DELETE',
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(controller.deleteFeed('feed_123')).rejects.toThrow(
        /Not Found/
      );
    });
  });

  describe('sourceUrl computation', () => {
    async function listFeedsWithSourceType(
      sourceType: string,
      sourceFeedId: string | undefined
    ) {
      mockRequest.mockResolvedValueOnce({
        data: [
          {
            id: 'feed_1',
            name: 'Feed',
            source_type: sourceType,
            source_feed_id: sourceFeedId,
          },
        ],
      });
      const controller = new FeedsController();
      const [feed] = await controller.listFeeds();
      return feed.sourceUrl;
    }

    it('bcfy_feeds produces the partner.broadcastify.com URL', async () => {
      const url = await listFeedsWithSourceType('bcfy_feeds', '12345');
      expect(url).toBe('https://partner.broadcastify.com/12345');
    });

    it('bcfy_calls replaces hyphens with slashes in the URL', async () => {
      const url = await listFeedsWithSourceType('bcfy_calls', '12-345-678');
      expect(url).toBe('https://www.broadcastify.com/calls/tg/12/345/678');
    });

    it('bcfy_calls with no hyphens in sourceFeedId', async () => {
      const url = await listFeedsWithSourceType('bcfy_calls', '12345');
      expect(url).toBe('https://www.broadcastify.com/calls/tg/12345');
    });

    it('openmhz produces the openmhz.com URL', async () => {
      const url = await listFeedsWithSourceType('openmhz', 'my-system');
      expect(url).toBe('https://openmhz.com/system/my-system');
    });

    it('echo always produces undefined', async () => {
      const url = await listFeedsWithSourceType('echo', 'some-id');
      expect(url).toBeUndefined();
    });

    it('produces undefined when sourceFeedId is absent', async () => {
      const url = await listFeedsWithSourceType('bcfy_feeds', undefined);
      expect(url).toBeUndefined();
    });
  });

  describe('archiveUrl computation', () => {
    async function listFeedsArchiveUrl(
      sourceType: string,
      sourceFeedId: string | undefined
    ) {
      mockRequest.mockResolvedValueOnce({
        data: [
          {
            id: 'feed_1',
            name: 'Feed',
            source_type: sourceType,
            source_feed_id: sourceFeedId,
          },
        ],
      });
      const controller = new FeedsController();
      const [feed] = await controller.listFeeds();
      return feed.archiveUrl;
    }

    it('bcfy_feeds produces the archives URL', async () => {
      const url = await listFeedsArchiveUrl('bcfy_feeds', '12345');
      expect(url).toBe('https://www.broadcastify.com/archives/feed/12345');
    });

    it('bcfy_calls produces the archives URL', async () => {
      const url = await listFeedsArchiveUrl('bcfy_calls', '12345');
      expect(url).toBe('https://www.broadcastify.com/calls/tg/12345/archives');
    });

    it('openmhz produces undefined', async () => {
      const url = await listFeedsArchiveUrl('openmhz', 'my-system');
      expect(url).toBeUndefined();
    });

    it('echo produces undefined', async () => {
      const url = await listFeedsArchiveUrl('echo', 'some-id');
      expect(url).toBeUndefined();
    });

    it('produces undefined when sourceFeedId is absent', async () => {
      const url = await listFeedsArchiveUrl('bcfy_feeds', undefined);
      expect(url).toBeUndefined();
    });
  });
});
