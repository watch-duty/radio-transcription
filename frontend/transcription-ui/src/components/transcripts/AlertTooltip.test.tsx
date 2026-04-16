// @vitest-environment jsdom
import { afterEach, describe, expect, it } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import AlertTooltip from './AlertTooltip';

describe('AlertTooltip', () => {
  const mockRuleIdToNameMap = new Map<string, string>([
    ['rule1', 'Critical Alert Rule'],
    ['rule2', 'Amber Warning Rule'],
  ]);

  afterEach(() => {
    cleanup();
  });

  it('should render null if evaluationDecisions is empty', () => {
    const { container } = render(
      <AlertTooltip
        evaluationDecisions={[]}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={false}
      />
    );
    expect(container.firstChild).toBeNull();
  });

  it('should render null if evaluationDecisions is undefined', () => {
    const { container } = render(
      <AlertTooltip
        evaluationDecisions={undefined as unknown as string[]}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={false}
      />
    );
    expect(container.firstChild).toBeNull();
  });

  it('should render warning icon if evaluationDecisions has items', () => {
    render(
      <AlertTooltip
        evaluationDecisions={['rule1']}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={false}
      />
    );
    expect(screen.getByTestId('warning-icon')).toBeTruthy();
  });

  it('should render rule names correctly when rulesLoading is false', async () => {
    render(
      <AlertTooltip
        evaluationDecisions={['rule1', 'rule2']}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={false}
      />
    );

    const icon = screen.getByTestId('warning-icon');
    fireEvent.mouseOver(icon);

    await waitFor(() => {
      expect(screen.getByText('Critical Alert Rule')).toBeTruthy();
      expect(screen.getByText('Amber Warning Rule')).toBeTruthy();
    });
  });

  it('should render rule IDs correctly when rulesLoading is true', async () => {
    render(
      <AlertTooltip
        evaluationDecisions={['rule1', 'rule2']}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={true}
      />
    );

    const icon = screen.getByTestId('warning-icon');
    fireEvent.mouseOver(icon);

    await waitFor(() => {
      expect(screen.getByText('rule1')).toBeTruthy();
      expect(screen.getByText('rule2')).toBeTruthy();
    });
  });

  it('should fallback to ruleId if name not in map and rulesLoading is false', async () => {
    render(
      <AlertTooltip
        evaluationDecisions={['unknown-rule']}
        ruleIdToNameMap={mockRuleIdToNameMap}
        rulesLoading={false}
      />
    );

    const icon = screen.getByTestId('warning-icon');
    fireEvent.mouseOver(icon);

    await waitFor(() => {
      expect(screen.getByText('unknown-rule')).toBeTruthy();
    });
  });
});
