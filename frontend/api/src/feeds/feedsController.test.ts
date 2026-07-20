import type * as express from 'express';

import { SourceType } from '@transcription/common';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FeedsController } from './feedsController.js';

// Mock the config module
vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  FEEDS_STORE_API_URL: 'http://feeds-api.example.com',
}));

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

describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const mockBackendFeed = {
    id: 'feed_123',
    name: 'Test Feed',
    source_type: 'openmhz',
    source_feed_id: 'src_123',
    status: 'active',
    substatus: 'active',
    last_heartbeat: '2024-01-01T00:00:00Z',
  };

  const expectedFrontendFeed = {
    id: 'feed_123',
    name: 'Test Feed',
    sourceType: 'openmhz',
    sourceFeedId: 'src_123',
    sourceUrl: 'https://openmhz.com/system/src_123',
    archiveUrl: undefined,
    status: 'active',
    substatus: 'active',
    lastHeartbeat: Date.parse('2024-01-01T00:00:00Z'),
  };

  const mockAdminRequest = {
    user: { isAdmin: true, email: 'admin@example.com' },
  } as unknown as express.Request;

  const mockNonAdminRequest = {
    user: { isAdmin: false },
  } as unknown as express.Request;

  const expectedActorHeaders = {
    'X-WD-Actor-Id': 'user:google:admin@example.com',
  };

  const malformedAdminRequests = [
    ['missing email', { user: { isAdmin: true } }],
    ['empty email', { user: { isAdmin: true, email: '' } }],
    ['blank email', { user: { isAdmin: true, email: '   ' } }],
    [
      'space in email',
      { user: { isAdmin: true, email: 'admin @example.com' } },
    ],
  ] as const;

  function asExpressRequest(request: unknown): express.Request {
    return request as express.Request;
  }

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
      expect(mockRequest.mock.calls[0][0]).not.toHaveProperty('headers');
    });

    it('should return feeds with tags on success', async () => {
      const mockFeedWithTags = {
        ...mockBackendFeed,
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      mockRequest.mockResolvedValueOnce({ data: [mockFeedWithTags] });

      const controller = new FeedsController();
      const result = await controller.listFeeds();

      expect(result).toEqual([
        {
          ...expectedFrontendFeed,
          tags: [{ key: 'county', value: 'Fulton' }],
        },
      ]);
    });

    it('should map backend status_reason_detail to frontend statusReasonDetail', async () => {
      mockRequest.mockResolvedValueOnce({
        data: [
          {
            ...mockBackendFeed,
            status_reason_detail: 'provider timeout',
          },
        ],
      });

      const controller = new FeedsController();
      const result = await controller.listFeeds();

      expect(result).toEqual([
        {
          ...expectedFrontendFeed,
          statusReasonDetail: 'provider timeout',
        },
      ]);
    });

    it('should throw error on API failure', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));
      const controller = new FeedsController();

      await expect(controller.listFeeds()).rejects.toThrow(/Network Error/);
    });

    it('should return converted feeds when backend returns object response', async () => {
      mockRequest.mockResolvedValueOnce({
        data: {
          feeds: [mockBackendFeed],
          next_token: 'token_123',
          total: 10,
        },
      });

      const controller = new FeedsController();
      const result = await controller.listFeeds();

      expect(result).toEqual({
        feeds: [expectedFrontendFeed],
        nextToken: 'token_123',
        total: 10,
      });
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com',
        method: 'GET',
      });
    });

    it('should pass query parameters to backend on request', async () => {
      mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });

      const controller = new FeedsController();
      const query = {
        limit: 10,
        nextToken: 'token_abc',
        order: 'asc' as const,
        sourceTypes: `${SourceType.OPENMHZ},${SourceType.ECHO}`,
        statuses: 'active',
        tags: [
          '{ "key": "region", "value": "West" }',
          '{ "key": "county", "value": "Fulton" }',
        ],
      };
      await controller.listFeeds(query);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com?limit=10&next_token=token_abc&order=asc&source_types=openmhz%2Cecho&statuses=active&tags=%5B%7B%22key%22%3A%22region%22%2C%22value%22%3A%22West%22%7D%2C%7B%22key%22%3A%22county%22%2C%22value%22%3A%22Fulton%22%7D%5D',
        method: 'GET',
      });
    });

    it('should reject malformed tag filters before calling backend', async () => {
      const controller = new FeedsController();

      await expect(
        controller.listFeeds({ tags: ['not-json'] })
      ).rejects.toMatchObject({
        status: 400,
        message: 'Invalid tags query parameter',
      });
      expect(mockRequest).not.toHaveBeenCalled();
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
      expect(mockRequest.mock.calls[0][0]).not.toHaveProperty('headers');
    });

    it('should return feed with tags on success', async () => {
      const mockFeedWithTags = {
        ...mockBackendFeed,
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      mockRequest.mockResolvedValueOnce({ data: mockFeedWithTags });

      const controller = new FeedsController();
      const result = await controller.getFeed('feed_123');

      expect(result).toEqual({
        ...expectedFrontendFeed,
        tags: [{ key: 'county', value: 'Fulton' }],
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
        sourceType: SourceType.OPENMHZ,
        sourceFeedId: 'src_123',
      };
      const result = await controller.createFeed(mockAdminRequest, payload);

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com',
        method: 'POST',
        headers: expectedActorHeaders,
        data: {
          name: 'Test Feed',
          source_type: 'openmhz',
          source_feed_id: 'src_123',
        },
      });
    });

    it('should create feed with tags on success', async () => {
      const mockFeedWithTags = {
        ...mockBackendFeed,
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      mockRequest.mockResolvedValueOnce({ data: mockFeedWithTags });

      const controller = new FeedsController();
      const payload = {
        name: 'Test Feed',
        sourceType: SourceType.OPENMHZ,
        sourceFeedId: 'src_123',
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      const result = await controller.createFeed(mockAdminRequest, payload);

      expect(result).toEqual({
        ...expectedFrontendFeed,
        tags: [{ key: 'county', value: 'Fulton' }],
      });
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com',
        method: 'POST',
        headers: expectedActorHeaders,
        data: {
          name: 'Test Feed',
          source_type: 'openmhz',
          source_feed_id: 'src_123',
          tags: [{ key: 'county', value: 'Fulton' }],
        },
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new FeedsController();
      const payload = {
        name: 'Test Feed',
        sourceType: SourceType.OPENMHZ,
        sourceFeedId: 'src_123',
        externalId: 'ext_123',
      };
      await expect(
        controller.createFeed(mockNonAdminRequest, payload)
      ).rejects.toThrow(/Forbidden/);
    });
  });

  describe('updateFeed', () => {
    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const payload = {
        name: 'Updated Feed',
      };
      const result = await controller.updateFeed(
        mockAdminRequest,
        'feed_123',
        payload
      );

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'PUT',
        headers: expectedActorHeaders,
        data: {
          name: 'Updated Feed',
          tags: undefined,
        },
      });
    });

    it('should update feed with tags on success', async () => {
      const mockFeedWithTags = {
        ...mockBackendFeed,
        name: 'Updated Feed',
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      mockRequest.mockResolvedValueOnce({ data: mockFeedWithTags });

      const controller = new FeedsController();
      const payload = {
        name: 'Updated Feed',
        tags: [{ key: 'county', value: 'Fulton' }],
      };
      const result = await controller.updateFeed(
        mockAdminRequest,
        'feed_123',
        payload
      );

      expect(result).toEqual({
        ...expectedFrontendFeed,
        name: 'Updated Feed',
        tags: [{ key: 'county', value: 'Fulton' }],
      });
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'PUT',
        headers: expectedActorHeaders,
        data: {
          name: 'Updated Feed',
          tags: [{ key: 'county', value: 'Fulton' }],
        },
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new FeedsController();
      const payload = {
        name: 'Updated Feed',
        externalId: 'ext_123',
      };
      await expect(
        controller.updateFeed(mockNonAdminRequest, 'feed_123', payload)
      ).rejects.toThrow(/Forbidden/);
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      const payload = {
        name: 'Updated Feed',
      };
      await expect(
        controller.updateFeed(mockAdminRequest, 'feed_123', payload)
      ).rejects.toThrow(/Not Found/);
    });

    it('should throw on non-404 API error', async () => {
      const error = new Error('Server Error') as Error & {
        response?: { status: number; data?: unknown };
      };
      error.response = { status: 500, data: 'Internal Server Error' };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      const payload = {
        name: 'Updated Feed',
      };
      await expect(
        controller.updateFeed(mockAdminRequest, 'feed_123', payload)
      ).rejects.toThrow(/Server Error/);
    });
  });

  describe('resetFeed', () => {
    const mockAdminRequest = {
      user: { isAdmin: true, email: 'admin@example.com' },
    } as unknown as express.Request;

    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.resetFeed('feed_123', mockAdminRequest);

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/reset',
        method: 'POST',
        headers: expectedActorHeaders,
      });
    });

    it('should throw 401 unauthorized if the user is not an admin', async () => {
      const mockNonAdminReq = {
        user: { isAdmin: false },
      } as unknown as express.Request;
      const controller = new FeedsController();

      await expect(
        controller.resetFeed('feed_123', mockNonAdminReq)
      ).rejects.toThrow(/Forbidden/);
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(
        controller.resetFeed('feed_123', mockAdminRequest)
      ).rejects.toThrow(/Not Found/);
    });

    it('should throw on non-404 API error', async () => {
      const error = new Error('Server Error') as Error & {
        response?: { status: number; data?: unknown };
      };
      error.response = { status: 500, data: 'Internal Server Error' };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(
        controller.resetFeed('feed_123', mockAdminRequest)
      ).rejects.toThrow(/Server Error/);
    });

    it('should throw on unexpected error', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));

      const controller = new FeedsController();
      await expect(
        controller.resetFeed('feed_123', mockAdminRequest)
      ).rejects.toThrow(/Network Error/);
    });
  });

  describe('deactivateFeed', () => {
    const mockAdminRequest = {
      user: { isAdmin: true, email: 'admin@example.com' },
    } as unknown as express.Request;

    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deactivateFeed('feed_123', mockAdminRequest);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/deactivate',
        method: 'POST',
        headers: expectedActorHeaders,
      });
    });

    it('should throw 401 unauthorized if the user is not an admin', async () => {
      const mockNonAdminReq = {
        user: { isAdmin: false },
      } as unknown as express.Request;
      const controller = new FeedsController();

      await expect(
        controller.deactivateFeed('feed_123', mockNonAdminReq)
      ).rejects.toThrow(/Forbidden/);
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(
        controller.deactivateFeed('feed_123', mockAdminRequest)
      ).rejects.toThrow(/Not Found/);
    });
  });

  describe('deleteFeed', () => {
    const mockAdminRequest = {
      user: { isAdmin: true, email: 'admin@example.com' },
    } as unknown as express.Request;

    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deleteFeed('feed_123', mockAdminRequest);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'DELETE',
        headers: expectedActorHeaders,
      });
    });

    it('should throw 401 unauthorized if the user is not an admin', async () => {
      const mockNonAdminReq = {
        user: { isAdmin: false },
      } as unknown as express.Request;
      const controller = new FeedsController();

      await expect(
        controller.deleteFeed('feed_123', mockNonAdminReq)
      ).rejects.toThrow(/Forbidden/);
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(
        controller.deleteFeed('feed_123', mockAdminRequest)
      ).rejects.toThrow(/Not Found/);
    });
  });

  describe('admin mutation actor forwarding', () => {
    const createPayload = {
      name: 'Test Feed',
      sourceType: SourceType.OPENMHZ,
      sourceFeedId: 'src_123',
    };
    const updatePayload = {
      name: 'Updated Feed',
    };

    it.each(malformedAdminRequests)(
      'createFeed rejects %s before calling backend',
      async (_label, request) => {
        const controller = new FeedsController();

        await expect(
          controller.createFeed(asExpressRequest(request), createPayload)
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );

    it.each(malformedAdminRequests)(
      'updateFeed rejects %s before calling backend',
      async (_label, request) => {
        const controller = new FeedsController();

        await expect(
          controller.updateFeed(
            asExpressRequest(request),
            'feed_123',
            updatePayload
          )
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );

    it.each(malformedAdminRequests)(
      'resetFeed rejects %s before calling backend',
      async (_label, request) => {
        const controller = new FeedsController();

        await expect(
          controller.resetFeed('feed_123', asExpressRequest(request))
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );

    it.each(malformedAdminRequests)(
      'deactivateFeed rejects %s before calling backend',
      async (_label, request) => {
        const controller = new FeedsController();

        await expect(
          controller.deactivateFeed('feed_123', asExpressRequest(request))
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );

    it.each(malformedAdminRequests)(
      'deleteFeed rejects %s before calling backend',
      async (_label, request) => {
        const controller = new FeedsController();

        await expect(
          controller.deleteFeed('feed_123', asExpressRequest(request))
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );
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
      const result = await controller.listFeeds();
      const feeds = Array.isArray(result) ? result : result.feeds;
      const [feed] = feeds;
      return feed.sourceUrl;
    }

    it('bcfy_feeds produces the listen/feed URL', async () => {
      const url = await listFeedsWithSourceType('bcfy_feeds', '12345');
      expect(url).toBe('https://www.broadcastify.com/listen/feed/12345');
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

    it('echo produces the GCS storage index URL with sourceFeedId hash and trailing slash', async () => {
      const url = await listFeedsWithSourceType(
        'echo',
        'Yakima_Co_LV_Fire_Disp-rapid_deploy-16'
      );
      expect(url).toBe(
        'https://storage.googleapis.com/wd-echo-recordings-prod/index.html#Yakima_Co_LV_Fire_Disp-rapid_deploy-16/'
      );
    });

    it('fire_notifications produces the audioplay URL', async () => {
      const url = await listFeedsWithSourceType(
        'fire_notifications',
        'RECORDINGS/WA-SPOKANE-DISP'
      );
      expect(url).toBe(
        'https://audioplay.textmefires.info/audioplay/folder_play?dir=RECORDINGS%2FWA-SPOKANE-DISP'
      );
    });

    it('fire_notifications with leading slash produces the audioplay URL', async () => {
      const url = await listFeedsWithSourceType(
        'fire_notifications',
        '/RECORDINGS/WA-SPOKANE-DISP'
      );
      expect(url).toBe(
        'https://audioplay.textmefires.info/audioplay/folder_play?dir=RECORDINGS%2FWA-SPOKANE-DISP'
      );
    });

    it('produces undefined when sourceFeedId is absent', async () => {
      const url = await listFeedsWithSourceType('bcfy_feeds', undefined);
      expect(url).toBeUndefined();
    });

    it('returns undefined for unknown source type', async () => {
      const url = await listFeedsWithSourceType(
        'unknown' as SourceType,
        'some-id'
      );
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
      const result = await controller.listFeeds();
      const feeds = Array.isArray(result) ? result : result.feeds;
      const [feed] = feeds;
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

    it('fire_notifications produces the archive URL', async () => {
      const url = await listFeedsArchiveUrl(
        'fire_notifications',
        'RECORDINGS/WA-SPOKANE-DISP'
      );
      expect(url).toBe(
        'https://audioplay.textmefires.info/audioplay/folder_play?dir=RECORDINGS%2FWA-SPOKANE-DISP%2FArchive'
      );
    });

    it('fire_notifications with leading slash produces the archive URL', async () => {
      const url = await listFeedsArchiveUrl(
        'fire_notifications',
        '/RECORDINGS/WA-SPOKANE-DISP'
      );
      expect(url).toBe(
        'https://audioplay.textmefires.info/audioplay/folder_play?dir=RECORDINGS%2FWA-SPOKANE-DISP%2FArchive'
      );
    });

    it('produces undefined when sourceFeedId is absent', async () => {
      const url = await listFeedsArchiveUrl('bcfy_feeds', undefined);
      expect(url).toBeUndefined();
    });

    it('returns undefined for unknown source type', async () => {
      const url = await listFeedsArchiveUrl('unknown' as SourceType, 'some-id');
      expect(url).toBeUndefined();
    });
  });

  describe('listFeedHistory', () => {
    it('should return converted history events on success', async () => {
      const mockBackendEvent = {
        id: 'evt_123',
        feed_id: 'feed_123',
        action: 'feed.recovered',
        actor: 'user:google:admin@example.com',
        occurred_at: '2026-06-26T12:34:56.000Z',
        feed_revision_num: 2,
        before_values: { status: 'failing' },
        after_values: { status: 'active' },
      };

      mockRequest.mockResolvedValueOnce({
        data: {
          history_events: [mockBackendEvent],
          next_token: 'token_next',
          total: 1,
        },
      });

      const controller = new FeedsController();
      const query = {
        limit: 10,
        nextToken: 'token_abc',
        order: 'asc' as const,
      };

      const result = await controller.listFeedHistory(
        mockAdminRequest,
        'feed_123',
        query
      );

      expect(result).toEqual({
        historyEvents: [
          {
            id: 'evt_123',
            feedId: 'feed_123',
            action: 'feed.recovered',
            actor: 'user:google:admin@example.com',
            occurredAt: Date.parse('2026-06-26T12:34:56.000Z'),
            feedRevision: 2,
            beforeValues: { status: 'failing' },
            afterValues: { status: 'active' },
          },
        ],
        nextToken: 'token_next',
        total: 1,
      });

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/history?limit=10&next_token=token_abc&order=asc',
        method: 'GET',
      });
    });

    it('should succeed and return history events for a non-admin user', async () => {
      const mockBackendEvent = {
        id: 'evt_123',
        feed_id: 'feed_123',
        action: 'feed.recovered',
        actor: 'user:google:admin@example.com',
        occurred_at: '2026-06-26T12:34:56.000Z',
        feed_revision_num: 2,
        before_values: { status: 'failing' },
        after_values: { status: 'active' },
      };

      mockRequest.mockResolvedValueOnce({
        data: {
          history_events: [mockBackendEvent],
          next_token: 'token_next',
          total: 1,
        },
      });

      const controller = new FeedsController();
      const result = await controller.listFeedHistory(
        mockNonAdminRequest,
        'feed_123',
        { limit: 10 }
      );

      expect(result).toEqual({
        historyEvents: [
          {
            id: 'evt_123',
            feedId: 'feed_123',
            action: 'feed.recovered',
            actor: 'user:google:admin@example.com',
            occurredAt: Date.parse('2026-06-26T12:34:56.000Z'),
            feedRevision: 2,
            beforeValues: { status: 'failing' },
            afterValues: { status: 'active' },
          },
        ],
        nextToken: 'token_next',
        total: 1,
      });

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/history?limit=10',
        method: 'GET',
      });
    });

    it('should throw HTTP error if backend fails', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new FeedsController();
      await expect(
        controller.listFeedHistory(mockAdminRequest, 'feed_123', {
          limit: 10,
        })
      ).rejects.toThrow(/Not Found/);
    });
  });

  describe('status conversion', () => {
    const testCases = [
      { backend: 'active', expected: 'active' },
      { backend: 'failing', expected: 'error' },
      { backend: 'unclaimed', expected: 'inactive' },
      { backend: 'quarantined', expected: 'error' },
      { backend: 'deactivated', expected: 'inactive' },
    ];

    testCases.forEach(({ backend, expected }) => {
      it(`should convert ${backend} to ${expected}`, async () => {
        mockRequest.mockResolvedValueOnce({
          data: [{ ...mockBackendFeed, status: backend }],
        });

        const controller = new FeedsController();
        const result = await controller.listFeeds();
        const feeds = Array.isArray(result) ? result : result.feeds;
        const [feed] = feeds;

        expect(feed.status).toBe(expected);
      });
    });
  });
});
