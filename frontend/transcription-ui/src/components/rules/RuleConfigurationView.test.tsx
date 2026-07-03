// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { type Feed, type Rule, SourceType } from '@transcription/common';

import { createRule } from '../../service/createRule';
import { deleteRule } from '../../service/deleteRule';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { updateRule } from '../../service/updateRule';
import { renderWithQueryClient } from '../../test/testUtils';
import RuleConfigurationView from './RuleConfigurationView';

// Mock API services
vi.mock('../../service/listRules', () => ({
  listRules: vi.fn(),
}));

vi.mock('../../service/createRule', () => ({
  createRule: vi.fn(),
}));

vi.mock('../../service/updateRule', () => ({
  updateRule: vi.fn(),
}));

vi.mock('../../service/deleteRule', () => ({
  deleteRule: vi.fn(),
}));

vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

// Mock AuthContext
const mockUseAuth = vi.fn();
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => mockUseAuth(),
}));

describe('RuleConfigurationView', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockOnError = vi.fn();

  const mockFeeds: Feed[] = [
    {
      id: 'feed-1',
      name: 'Marin Fire Dispatch',
      sourceType: SourceType.BCFY_FEEDS,
      sourceFeedId: '33156',
      status: 'active',
      substatus: 'active',
      tags: [{ key: 'county', value: 'Marin' }],
    },
  ];

  const mockRules: Rule[] = [
    {
      ruleId: 'rule-1',
      ruleName: 'Evacuation Trigger',
      description: 'Matches evacuation calls',
      isActive: true,
      scope: {
        level: 'GLOBAL',
        targetFeeds: [],
      },
      conditions: {
        evaluationType: 'KEYWORD_MATCH',
        operator: 'ANY',
        keywords: ['evacuate', 'evacuation'],
        caseSensitive: false,
      },
      metadata: {
        createdAt: '2026-06-03T00:00:00Z',
        updatedAt: '2026-06-03T00:00:00Z',
      },
    },
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    mockTriggerSnackbar.mockClear();
    mockOnError.mockClear();

    mockUseAuth.mockReturnValue({ token: 'fake-jwt-token-xyz', isAdmin: true });
    vi.mocked(listRules).mockResolvedValue(mockRules);
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: mockFeeds,
      total: mockFeeds.length,
    });
    vi.mocked(deleteRule).mockResolvedValue(undefined);

    window.scrollTo = vi.fn();
  });

  afterEach(() => {
    cleanup();
  });

  const renderView = () => {
    return renderWithQueryClient(
      <MemoryRouter>
        <VirtuosoMockContext.Provider
          value={{ viewportHeight: 1000, itemHeight: 100 }}
        >
          <RuleConfigurationView
            triggerSnackbar={mockTriggerSnackbar}
            onError={mockOnError}
          />
        </VirtuosoMockContext.Provider>
      </MemoryRouter>
    );
  };

  describe('As Admin', () => {
    it('renders both the creation form and the existing rules list', async () => {
      renderView();

      expect(
        screen.getByRole('heading', { name: 'Rules', level: 4 })
      ).toBeInTheDocument();
      expect(screen.getByText('Create New Rule')).toBeInTheDocument();

      const formCard = screen.getByTestId('rule-config-card');
      expect(within(formCard).getByLabelText('Rule Name')).toBeInTheDocument();
      expect(
        within(formCard).getByLabelText('Description (Optional)')
      ).toBeInTheDocument();
      expect(
        within(formCard).getByLabelText('Scope Level')
      ).toBeInTheDocument();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });
    });

    it('validates empty required fields and displays interactive errors', async () => {
      renderView();

      const submitBtn = screen.getByRole('button', {
        name: /Create Rule/i,
      });
      fireEvent.click(submitBtn);

      expect(screen.getByText('Rule name is required.')).toBeInTheDocument();
      expect(createRule).not.toHaveBeenCalled();
    });

    it('supports adding and deleting keywords interactively', async () => {
      renderView();

      const keywordInput = screen.getByLabelText('Add Keywords');
      const addKeywordBtn = screen.getByRole('button', { name: 'Add' });

      // Add a single keyword
      fireEvent.change(keywordInput, { target: { value: 'dispatch' } });
      fireEvent.click(addKeywordBtn);

      expect(screen.getByText('dispatch')).toBeInTheDocument();
      expect(keywordInput).toHaveValue('');

      // Add multiple comma-separated keywords
      fireEvent.change(keywordInput, { target: { value: 'structure, alarm' } });
      fireEvent.click(addKeywordBtn);

      expect(screen.getByText('structure')).toBeInTheDocument();
      expect(screen.getByText('alarm')).toBeInTheDocument();

      // Remove keyword
      const dispatchChip = screen.getByRole('button', { name: 'dispatch' });
      const deleteIcon = dispatchChip.querySelector('.MuiChip-deleteIcon');
      expect(deleteIcon).toBeInTheDocument();
      fireEvent.click(deleteIcon!);

      expect(screen.queryByText('dispatch')).not.toBeInTheDocument();
    });

    it('submits form successfully, registers rule in API, and clears state', async () => {
      const mockCreatedRule = {
        ruleId: 'rule-99',
        ruleName: 'Dispatch Alerts',
        isActive: true,
        scope: { level: 'GLOBAL' as const, targetFeeds: [] },
        conditions: {
          evaluationType: 'KEYWORD_MATCH' as const,
          operator: 'ANY' as const,
          keywords: ['dispatch'],
          caseSensitive: false,
        },
        metadata: { createdAt: '', updatedAt: '' },
      };
      vi.mocked(createRule).mockResolvedValue(mockCreatedRule);

      renderView();

      const formCard = screen.getByTestId('rule-config-card');

      fireEvent.change(within(formCard).getByLabelText('Rule Name'), {
        target: { value: 'Dispatch Alerts' },
      });

      fireEvent.change(within(formCard).getByLabelText('Add Keywords'), {
        target: { value: 'dispatch' },
      });

      const submitBtn = within(formCard).getByRole('button', {
        name: /Create Rule/i,
      });
      fireEvent.click(submitBtn);

      await waitFor(() => {
        expect(createRule).toHaveBeenCalledTimes(1);
        expect(createRule).toHaveBeenCalledWith(
          {
            ruleName: 'Dispatch Alerts',
            description: undefined,
            isActive: true,
            scope: { level: 'GLOBAL', targetFeeds: [] },
            conditions: {
              evaluationType: 'KEYWORD_MATCH',
              operator: 'ANY',
              keywords: ['dispatch'],
              caseSensitive: false,
            },
          },
          'fake-jwt-token-xyz'
        );

        expect(mockTriggerSnackbar).toHaveBeenCalledWith(
          'Rule "Dispatch Alerts" created successfully!'
        );
        expect(within(formCard).getByLabelText('Rule Name')).toHaveValue('');
      });
    });

    it('supports transitioning to edit mode, cancelling, and saving updates', async () => {
      const mockUpdatedRule = {
        ...mockRules[0],
        ruleName: 'Evacuation Triggers Mod',
        description: 'Matches active evacuations',
      };
      vi.mocked(updateRule).mockResolvedValue(mockUpdatedRule);

      renderView();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });

      const editBtn = screen.getByRole('button', {
        name: 'Edit Evacuation Trigger',
      });
      fireEvent.click(editBtn);

      expect(
        screen.getByText('Edit Rule: Evacuation Trigger')
      ).toBeInTheDocument();

      const editFormCard = screen.getByTestId('rule-config-card');
      expect(within(editFormCard).getByLabelText('Rule Name')).toHaveValue(
        'Evacuation Trigger'
      );

      // Test Cancel
      const cancelBtn = within(editFormCard).getByRole('button', {
        name: /Cancel Edit/i,
      });
      fireEvent.click(cancelBtn);

      expect(screen.getByText('Create New Rule')).toBeInTheDocument();

      // Re-enter edit and save
      fireEvent.click(editBtn);
      const editFormCard2 = screen.getByTestId('rule-config-card');
      fireEvent.change(within(editFormCard2).getByLabelText('Rule Name'), {
        target: { value: 'Evacuation Triggers Mod' },
      });

      const submitBtn = within(editFormCard2).getByRole('button', {
        name: /Save changes/i,
      });
      fireEvent.click(submitBtn);

      await waitFor(() => {
        expect(updateRule).toHaveBeenCalledTimes(1);
        expect(updateRule).toHaveBeenCalledWith(
          'rule-1',
          {
            ruleName: 'Evacuation Triggers Mod',
            description: 'Matches evacuation calls',
            isActive: true,
            scope: { level: 'GLOBAL', targetFeeds: [] },
            conditions: {
              evaluationType: 'KEYWORD_MATCH',
              operator: 'ANY',
              keywords: ['evacuate', 'evacuation'],
              caseSensitive: false,
            },
          },
          'fake-jwt-token-xyz'
        );
        expect(mockTriggerSnackbar).toHaveBeenCalledWith(
          'Rule "Evacuation Triggers Mod" updated successfully!'
        );
      });
    });

    it('supports delete action with confirmation dialog', async () => {
      renderView();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });

      const editBtn = screen.getByRole('button', {
        name: 'Edit Evacuation Trigger',
      });
      fireEvent.click(editBtn);

      const editFormCard = screen.getByTestId('rule-config-card');
      const kebabBtn = within(editFormCard).getByRole('button', {
        name: /rule actions/i,
      });
      fireEvent.click(kebabBtn);

      const deleteMenuItem = screen.getByRole('menuitem', {
        name: /Delete rule/i,
      });
      fireEvent.click(deleteMenuItem);

      expect(screen.getByText('Verify Rule Deletion')).toBeInTheDocument();

      const confirmDeleteBtn = screen.getByRole('button', { name: 'Delete' });
      expect(confirmDeleteBtn).toBeDisabled();

      const confirmInput = screen.getByPlaceholderText('Evacuation Trigger');
      fireEvent.change(confirmInput, {
        target: { value: 'Evacuation Trigger' },
      });
      expect(confirmDeleteBtn).toBeEnabled();

      fireEvent.click(confirmDeleteBtn);

      await waitFor(() => {
        expect(deleteRule).toHaveBeenCalledTimes(1);
        expect(deleteRule).toHaveBeenCalledWith('rule-1', 'fake-jwt-token-xyz');
        expect(mockTriggerSnackbar).toHaveBeenCalledWith(
          'Rule deleted successfully!'
        );
      });
    });

    it('supports toggle active status via dropdown menu', async () => {
      renderView();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });

      const editBtn = screen.getByRole('button', {
        name: 'Edit Evacuation Trigger',
      });
      fireEvent.click(editBtn);

      const editFormCard = screen.getByTestId('rule-config-card');
      const kebabBtn = within(editFormCard).getByRole('button', {
        name: /rule actions/i,
      });
      fireEvent.click(kebabBtn);

      const disableMenuItem = screen.getByRole('menuitem', {
        name: /Disable rule/i,
      });
      fireEvent.click(disableMenuItem);

      expect(screen.getByText('Verify Disabling Rule')).toBeInTheDocument();
      const confirmDisableBtn = screen.getByRole('button', { name: 'Disable' });
      fireEvent.click(confirmDisableBtn);

      await waitFor(() => {
        expect(updateRule).toHaveBeenCalledTimes(1);
        expect(updateRule).toHaveBeenCalledWith(
          'rule-1',
          {
            ruleName: 'Evacuation Trigger',
            description: 'Matches evacuation calls',
            isActive: false,
            scope: { level: 'GLOBAL', targetFeeds: [] },
            conditions: {
              evaluationType: 'KEYWORD_MATCH',
              operator: 'ANY',
              keywords: ['evacuate', 'evacuation'],
              caseSensitive: false,
            },
          },
          'fake-jwt-token-xyz'
        );
      });
    });
  });

  describe('As Reporter', () => {
    beforeEach(() => {
      mockUseAuth.mockReturnValue({
        token: 'fake-jwt-token-xyz',
        isAdmin: false,
      });
    });

    it('renders only the rules list and hides the creation form', async () => {
      renderView();

      expect(
        screen.getByRole('heading', { name: 'Rules', level: 4 })
      ).toBeInTheDocument();
      expect(screen.queryByText('Create New Rule')).not.toBeInTheDocument();
      expect(screen.queryByTestId('rule-config-card')).not.toBeInTheDocument();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });
    });

    it('does not display edit buttons for rules', async () => {
      renderView();

      await waitFor(() => {
        expect(screen.getByText('Evacuation Trigger')).toBeInTheDocument();
      });

      expect(
        screen.queryByRole('button', { name: 'Edit Evacuation Trigger' })
      ).not.toBeInTheDocument();
    });
  });
});
