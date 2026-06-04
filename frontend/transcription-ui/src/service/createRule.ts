import type { Rule, RuleCreate } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function createRule(
  rule: RuleCreate,
  token: string
): Promise<Rule> {
  return apiFetch<Rule>(`${import.meta.env.VITE_API_BASE_URL}/api/v1/rules`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(rule),
  });
}
