import type * as express from 'express';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RulesController } from './rulesController.js';

// Mock the config module
vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  RULES_API_URL: 'http://rules-api.example.com',
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

describe('RulesController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockRequest.mockReset();
  });

  const mockBackendRule = {
    rule_id: 'rule_123',
    rule_name: 'Test Rule',
    description: 'Test Description',
    is_active: true,
    scope: {
      level: 'GLOBAL',
      target_feeds: [],
    },
    conditions: {
      evaluation_type: 'KEYWORD_MATCH',
      operator: 'ANY',
      keywords: ['test'],
      case_sensitive: false,
    },
    tags: [{ key: 'geo_event_type', value: 'flooding' }],
    metadata: {
      created_by: 'test@example.com',
      created_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
    },
  };

  const expectedFrontendRule = {
    ruleId: 'rule_123',
    ruleName: 'Test Rule',
    description: 'Test Description',
    isActive: true,
    scope: {
      level: 'GLOBAL',
      targetFeeds: [],
    },
    conditions: {
      evaluationType: 'KEYWORD_MATCH',
      operator: 'ANY',
      keywords: ['test'],
      caseSensitive: false,
    },
    tags: [{ key: 'geo_event_type', value: 'flooding' }],
    metadata: {
      createdBy: 'test@example.com',
      createdAt: '2026-01-01T00:00:00Z',
      updatedAt: '2026-01-01T00:00:00Z',
    },
  };

  describe('listRules', () => {
    it('should return converted rules on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: [mockBackendRule] });

      const controller = new RulesController();
      const result = await controller.listRules({});

      expect(result).toEqual([expectedFrontendRule]);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com',
        method: 'GET',
        params: expect.any(URLSearchParams),
      });
    });

    it('should throw error on API failure', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));
      const controller = new RulesController();

      await expect(controller.listRules({})).rejects.toThrow(/Network Error/);
    });
  });

  describe('getRule', () => {
    it('should return converted rule on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendRule });

      const controller = new RulesController();
      const result = await controller.getRule('rule_123');

      expect(result).toEqual(expectedFrontendRule);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'GET',
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new RulesController();
      await expect(controller.getRule('rule_123')).rejects.toThrow(/Not Found/);
    });
  });

  const mockAdminRequest = {
    user: {
      isAdmin: true,
    },
  } as unknown as express.Request;

  const mockNonAdminRequest = {
    user: {
      isAdmin: false,
    },
  } as unknown as express.Request;

  describe('createRule', () => {
    it('should return converted rule on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendRule });

      const controller = new RulesController();
      const payload = {
        ruleName: 'Test Rule',
        scope: { level: 'GLOBAL' as const, targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH' as const,
          operator: 'ANY' as const,
          keywords: ['test'],
          caseSensitive: false,
        },
        tags: [{ key: 'geo_event_type', value: 'flooding' }],
      };
      const result = await controller.createRule(mockAdminRequest, payload);

      expect(result).toEqual(expectedFrontendRule);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com',
        method: 'POST',
        data: {
          rule_name: 'Test Rule',
          description: undefined,
          is_active: undefined,
          scope: { level: 'GLOBAL', target_feeds: [] },
          conditions: {
            evaluation_type: 'KEYWORD_MATCH',
            operator: 'ANY',
            keywords: ['test'],
            case_sensitive: false,
          },
          tags: [{ key: 'geo_event_type', value: 'flooding' }],
        },
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new RulesController();
      const payload = {
        ruleName: 'Test Rule',
        scope: { level: 'GLOBAL' as const, targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH' as const,
          operator: 'ANY' as const,
          keywords: ['test'],
          caseSensitive: false,
        },
      };
      await expect(
        controller.createRule(mockNonAdminRequest, payload)
      ).rejects.toThrow('Forbidden');
    });
  });

  describe('updateRule', () => {
    it('should return converted rule on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendRule });

      const controller = new RulesController();
      const payload = { ruleName: 'Updated Name' };
      const result = await controller.updateRule(
        mockAdminRequest,
        'rule_123',
        payload
      );

      expect(result).toEqual(expectedFrontendRule);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'PUT',
        data: { rule_name: 'Updated Name' },
      });
    });

    it('should send a tags-only update', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendRule });

      const controller = new RulesController();
      await controller.updateRule(mockAdminRequest, 'rule_123', {
        tags: [{ key: 'geo_event_type', value: 'wildfire' }],
      });

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'PUT',
        data: { tags: [{ key: 'geo_event_type', value: 'wildfire' }] },
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new RulesController();
      await expect(
        controller.updateRule(mockNonAdminRequest, 'rule_123', {})
      ).rejects.toThrow('Forbidden');
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new RulesController();
      await expect(
        controller.updateRule(mockAdminRequest, 'rule_123', {})
      ).rejects.toThrow(/Not Found/);
    });
  });

  describe('deleteRule', () => {
    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new RulesController();
      await controller.deleteRule(mockAdminRequest, 'rule_123');

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'DELETE',
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new RulesController();
      await expect(
        controller.deleteRule(mockNonAdminRequest, 'rule_123')
      ).rejects.toThrow('Forbidden');
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new RulesController();
      await expect(
        controller.deleteRule(mockAdminRequest, 'rule_123')
      ).rejects.toThrow(/Not Found/);
    });
  });
  describe('dryRunRule', () => {
    it('should map start/end to startIndex/endIndex on success', async () => {
      const mockBackendResponse = {
        rule_hit_count: 5,
        total_evaluated: 10,
        examples: [
          {
            audio_segment_id: 'seg_1',
            feed_id: 'feed_1',
            text: 'this is a test text',
            matched_spans: [
              {
                start: 10,
                end: 14,
                matched_text: 'test',
              },
            ],
          },
        ],
      };

      mockRequest.mockResolvedValueOnce({ data: mockBackendResponse });

      const controller = new RulesController();
      const payload = {
        rule: {
          ruleName: 'Test Rule',
          scope: { level: 'GLOBAL' as const, targetFeeds: [] },
          conditions: {
            evaluationType: 'KEYWORD_MATCH' as const,
            operator: 'ANY' as const,
            keywords: ['test'],
            caseSensitive: false,
          },
        },
      };

      const result = await controller.dryRunRule(mockAdminRequest, payload);

      expect(result).toEqual({
        hitCount: 5,
        totalEvaluated: 10,
        examples: [
          {
            audioSegmentId: 'seg_1',
            feedId: 'feed_1',
            text: 'this is a test text',
            matchedSpans: [
              {
                startIndex: 10,
                endIndex: 14,
                matchedText: 'test',
              },
            ],
          },
        ],
      });

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/dry-run',
        method: 'POST',
        data: {
          rule: {
            rule_name: 'Test Rule',
            description: undefined,
            is_active: undefined,
            scope: { level: 'GLOBAL', target_feeds: [] },
            conditions: {
              evaluation_type: 'KEYWORD_MATCH',
              operator: 'ANY',
              keywords: ['test'],
              case_sensitive: false,
            },
          },
        },
      });
    });

    it('should throw 403 Forbidden if user is not an admin', async () => {
      const controller = new RulesController();
      const payload = {
        rule: {
          ruleName: 'Test Rule',
          scope: { level: 'GLOBAL' as const, targetFeeds: [] },
          conditions: {
            evaluationType: 'KEYWORD_MATCH' as const,
            operator: 'ANY' as const,
            keywords: ['test'],
            caseSensitive: false,
          },
        },
      };
      await expect(
        controller.dryRunRule(mockNonAdminRequest, payload)
      ).rejects.toThrow('Forbidden');
    });

    it('should throw on backend error', async () => {
      mockRequest.mockRejectedValueOnce(new Error('Network Error'));
      const controller = new RulesController();
      const payload = {
        rule: {
          ruleName: 'Test Rule',
          scope: { level: 'GLOBAL' as const, targetFeeds: [] },
          conditions: {
            evaluationType: 'KEYWORD_MATCH' as const,
            operator: 'ANY' as const,
            keywords: ['test'],
            caseSensitive: false,
          },
        },
      };

      await expect(
        controller.dryRunRule(mockAdminRequest, payload)
      ).rejects.toThrow(/Network Error/);
    });
  });
});
