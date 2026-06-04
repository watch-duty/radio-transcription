import type { Rule, RuleUpdate } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function updateRule(
  ruleId: string,
  rule: RuleUpdate,
  token: string
): Promise<Rule> {
  return apiFetch<Rule>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/rules/${ruleId}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(rule),
    }
  );
}
