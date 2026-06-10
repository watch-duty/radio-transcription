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
      };
      const result = await controller.createRule(payload);

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
        },
      });
    });
  });

  describe('updateRule', () => {
    it('should return converted rule on success', async () => {
      mockRequest.mockResolvedValueOnce({ data: mockBackendRule });

      const controller = new RulesController();
      const payload = { ruleName: 'Updated Name' };
      const result = await controller.updateRule('rule_123', payload);

      expect(result).toEqual(expectedFrontendRule);
      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'PUT',
        data: { rule_name: 'Updated Name' },
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new RulesController();
      await expect(controller.updateRule('rule_123', {})).rejects.toThrow(
        /Not Found/
      );
    });
  });

  describe('deleteRule', () => {
    it('should return 204 on success', async () => {
      mockRequest.mockResolvedValueOnce({ status: 204 });

      const controller = new RulesController();
      await controller.deleteRule('rule_123');

      expect(mockRequest).toHaveBeenCalledWith({
        url: 'http://rules-api.example.com/rule_123',
        method: 'DELETE',
      });
    });

    it('should throw on 404', async () => {
      const error = new Error('Not Found') as Error & {
        response?: { status: number };
      };
      error.response = { status: 404 };
      mockRequest.mockRejectedValueOnce(error);

      const controller = new RulesController();
      await expect(controller.deleteRule('rule_123')).rejects.toThrow(
        /Not Found/
      );
    });
  });
});
