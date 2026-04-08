import axios from 'axios';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RulesController, ScopeLevel, EvaluationType } from './rulesController.js';

// Mock the config module to inject the value without touching process.env
vi.mock('../config.js', () => ({
  RULES_MANAGEMENT_API_URL: 'http://rules-api.example.com',
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

describe('RulesController', () => {
  let controller: RulesController;

  beforeEach(() => {
    vi.clearAllMocks();
    controller = new RulesController();
  });

  const mockRule = {
    ruleId: 'rule-1',
    ruleName: 'Test Rule',
    description: 'A test rule',
    isActive: true,
    scope: {
      level: ScopeLevel.GLOBAL,
    },
    conditions: {
      evaluationType: EvaluationType.KEYWORD_MATCH,
      keywords: ['test'],
    },
  };

  describe('listRules', () => {
    it('should return data on success', async () => {
      const mockData = [mockRule];
      vi.mocked(axios.get).mockResolvedValueOnce({ data: mockData });

      const result = await controller.listRules();

      expect(result).toEqual(mockData);
      expect(axios.get).toHaveBeenCalledWith('http://rules-api.example.com', {
        headers: { Authorization: 'Bearer mock-token' },
      });
    });

    it('should throw error on API failure', async () => {
      const errorMessage = 'Network Error';
      vi.mocked(axios.get).mockRejectedValueOnce(new Error(errorMessage));

      await expect(controller.listRules()).rejects.toThrow(errorMessage);
    });
  });

  describe('createRule', () => {
    it('should return created rule on success', async () => {
      const { ruleId, ...ruleCreate } = mockRule;
      vi.mocked(axios.post).mockResolvedValueOnce({ data: mockRule });

      const result = await controller.createRule(ruleCreate);

      expect(result).toEqual(mockRule);
      expect(axios.post).toHaveBeenCalledWith('http://rules-api.example.com', ruleCreate, {
        headers: { Authorization: 'Bearer mock-token' },
      });
    });

    it('should throw error on API failure', async () => {
      const { ruleId, ...ruleCreate } = mockRule;
      const errorMessage = 'Network Error';
      vi.mocked(axios.post).mockRejectedValueOnce(new Error(errorMessage));

      await expect(controller.createRule(ruleCreate)).rejects.toThrow(errorMessage);
    });
  });

  describe('getRule', () => {
    it('should return rule on success', async () => {
      vi.mocked(axios.get).mockResolvedValueOnce({ data: mockRule });
      const mockNotFound = vi.fn();

      const result = await controller.getRule('rule-1', mockNotFound);

      expect(result).toEqual(mockRule);
      expect(axios.get).toHaveBeenCalledWith('http://rules-api.example.com/rule-1', {
        headers: { Authorization: 'Bearer mock-token' },
      });
      expect(mockNotFound).not.toHaveBeenCalled();
    });

    it('should call notFound on 404', async () => {
      const mockError = new Error('Not Found');
      (mockError as any).isAxiosError = true;
      (mockError as any).response = { status: 404 };
      
      vi.mocked(axios.get).mockRejectedValueOnce(mockError);
      vi.mocked(axios.isAxiosError).mockReturnValueOnce(true);
      const mockNotFound = vi.fn();

      await controller.getRule('rule-1', mockNotFound);

      expect(mockNotFound).toHaveBeenCalledWith(404, { message: 'Rule rule-1 not found' });
    });

    it('should throw error on other API failures', async () => {
      const errorMessage = 'Network Error';
      vi.mocked(axios.get).mockRejectedValueOnce(new Error(errorMessage));
      const mockNotFound = vi.fn();

      await expect(controller.getRule('rule-1', mockNotFound)).rejects.toThrow(errorMessage);
    });
  });

  describe('updateRule', () => {
    it('should return updated rule on success', async () => {
      const ruleUpdate = { ruleName: 'Updated Name' };
      const updatedRule = { ...mockRule, ...ruleUpdate };
      vi.mocked(axios.put).mockResolvedValueOnce({ data: updatedRule });
      const mockNotFound = vi.fn();

      const result = await controller.updateRule('rule-1', ruleUpdate, mockNotFound);

      expect(result).toEqual(updatedRule);
      expect(axios.put).toHaveBeenCalledWith('http://rules-api.example.com/rule-1', ruleUpdate, {
        headers: { Authorization: 'Bearer mock-token' },
      });
    });

    it('should call notFound on 404', async () => {
      const ruleUpdate = { ruleName: 'Updated Name' };
      const mockError = new Error('Not Found');
      (mockError as any).isAxiosError = true;
      (mockError as any).response = { status: 404 };
      
      vi.mocked(axios.put).mockRejectedValueOnce(mockError);
      vi.mocked(axios.isAxiosError).mockReturnValueOnce(true);
      const mockNotFound = vi.fn();

      await controller.updateRule('rule-1', ruleUpdate, mockNotFound);

      expect(mockNotFound).toHaveBeenCalledWith(404, { message: 'Rule rule-1 not found' });
    });
  });

  describe('deleteRule', () => {
    it('should delete rule on success', async () => {
      vi.mocked(axios.delete).mockResolvedValueOnce({ status: 204 });
      const mockNotFound = vi.fn();

      await controller.deleteRule('rule-1', mockNotFound);

      expect(axios.delete).toHaveBeenCalledWith('http://rules-api.example.com/rule-1', {
        headers: { Authorization: 'Bearer mock-token' },
      });
      expect(mockNotFound).not.toHaveBeenCalled();
    });

    it('should call notFound on 404', async () => {
      const mockError = new Error('Not Found');
      (mockError as any).isAxiosError = true;
      (mockError as any).response = { status: 404 };
      
      vi.mocked(axios.delete).mockRejectedValueOnce(mockError);
      vi.mocked(axios.isAxiosError).mockReturnValueOnce(true);
      const mockNotFound = vi.fn();

      await controller.deleteRule('rule-1', mockNotFound);

      expect(mockNotFound).toHaveBeenCalledWith(404, { message: 'Rule rule-1 not found' });
    });
  });
});
