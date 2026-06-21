import { readFileSync } from 'node:fs';

import type * as express from 'express';

import { SourceType } from '@transcription/common';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  FeedsController,
  type ListFeedsQueryParams,
} from './feedsController.js';

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

async function expectHttpErrorStatus(
  action: () => Promise<unknown>,
  status: number,
  message: RegExp
) {
  try {
    await action();
    throw new Error('Expected action to reject');
  } catch (error) {
    expect(error).toMatchObject({ status });
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(message);
  }
}

describe('FeedsController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  const adminSub = 'admin-sub-123';
  const adminActorHeaders = {
    'X-WD-Actor-Id': `user:google:${adminSub}`,
  };
  const requestWithUser = (principal: Record<string, unknown>) =>
    ({ ['user']: principal }) as unknown as express.Request;

  const mockBackendFeed = {
    id: 'feed_123',
    name: 'Test Feed',
    source_type: 'openmhz',
    source_feed_id: 'src_123',
    status: 'active',
    substatus: 'active',
    last_heartbeat: '2024-01-01T00:00:00Z',
    status_reason_detail: 'provider timeout',
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
    lastHeartbeat: '2024-01-01T00:00:00Z',
    statusReasonDetail: 'provider timeout',
  };

  const mockAdminRequest = requestWithUser({ isAdmin: true, sub: adminSub });

  const mockNonAdminRequest = requestWithUser({ isAdmin: false });

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
          '{"key":"region","value":"West"}',
          '{"key":"county","value":"Fulton"}',
        ],
      } satisfies ListFeedsQueryParams;
      await controller.listFeeds(query);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com?limit=10&next_token=token_abc&order=asc&source_types=openmhz%2Cecho&statuses=active&tags=%5B%7B%22key%22%3A%22region%22%2C%22value%22%3A%22West%22%7D%2C%7B%22key%22%3A%22county%22%2C%22value%22%3A%22Fulton%22%7D%5D',
        method: 'GET',
      });
    });

    it('should forward a single JSON-list tags query parameter', async () => {
      mockRequest.mockResolvedValueOnce({ data: [mockBackendFeed] });

      const controller = new FeedsController();
      await controller.listFeeds({
        tags: ['[{"key":"region","value":"West"}]'],
      });

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com?tags=%5B%7B%22key%22%3A%22region%22%2C%22value%22%3A%22West%22%7D%5D',
        method: 'GET',
      });
    });

    it('should reject invalid tags JSON before backend request', async () => {
      const controller = new FeedsController();

      await expectHttpErrorStatus(
        () => controller.listFeeds({ tags: ['not-json'] }),
        400,
        /tags must be valid JSON/
      );
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

    it('maps canonical status detail', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.getFeed('feed_123');

      expect(result.statusReasonDetail).toBe('provider timeout');
    });
  });

  describe('OpenAPI feed contract', () => {
    it('publishes canonical status detail and tag query shape', () => {
      const openApiYaml = readFileSync(
        new URL('../../openapi.yaml', import.meta.url),
        'utf8'
      );

      expect(openApiYaml).toContain('statusReasonDetail:');
      expect(openApiYaml).toMatch(
        /ListFeedsQueryParams:[\s\S]*tags:\s*\n\s*items:\s*\n\s*type: string\s*\n\s*type: array/
      );
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
        headers: adminActorHeaders,
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
        headers: adminActorHeaders,
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

    it.each([
      ['missing', requestWithUser({ isAdmin: true })],
      ['empty', requestWithUser({ isAdmin: true, sub: '' })],
      ['whitespace-only', requestWithUser({ isAdmin: true, sub: '   ' })],
      [
        'containing whitespace',
        requestWithUser({ isAdmin: true, sub: 'admin sub' }),
      ],
      [
        'leading whitespace',
        requestWithUser({ isAdmin: true, sub: ' admin-sub' }),
      ],
    ])(
      'should throw 403 and skip downstream call when admin sub is %s',
      async (_caseName, requestLike) => {
        const controller = new FeedsController();
        const payload = {
          name: 'Test Feed',
          sourceType: SourceType.OPENMHZ,
          sourceFeedId: 'src_123',
        };

        await expect(
          controller.createFeed(requestLike, payload)
        ).rejects.toThrow(/Forbidden/);
        expect(mockRequest).not.toHaveBeenCalled();
      }
    );
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
        headers: adminActorHeaders,
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
        headers: adminActorHeaders,
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
    const mockAdminRequest = requestWithUser({ isAdmin: true, sub: adminSub });

    it('should return converted feed on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendFeed });

      const controller = new FeedsController();
      const result = await controller.resetFeed('feed_123', mockAdminRequest);

      expect(result).toEqual(expectedFrontendFeed);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/reset',
        method: 'POST',
        headers: adminActorHeaders,
      });
    });

    it('should throw 403 forbidden if the user is not an admin', async () => {
      const mockNonAdminReq = requestWithUser({ isAdmin: false });
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
    const mockAdminRequest = requestWithUser({ isAdmin: true, sub: adminSub });

    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deactivateFeed('feed_123', mockAdminRequest);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123/deactivate',
        method: 'POST',
        headers: adminActorHeaders,
      });
    });

    it('should throw 403 forbidden if the user is not an admin', async () => {
      const mockNonAdminReq = requestWithUser({ isAdmin: false });
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
    const mockAdminRequest = requestWithUser({ isAdmin: true, sub: adminSub });

    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new FeedsController();
      await controller.deleteFeed('feed_123', mockAdminRequest);

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://feeds-api.example.com/feed_123',
        method: 'DELETE',
        headers: adminActorHeaders,
      });
    });

    it('should throw 403 forbidden if the user is not an admin', async () => {
      const mockNonAdminReq = requestWithUser({ isAdmin: false });
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

    it('echo always produces undefined', async () => {
      const url = await listFeedsWithSourceType('echo', 'some-id');
      expect(url).toBeUndefined();
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
