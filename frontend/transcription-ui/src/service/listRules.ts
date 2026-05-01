import type { Rule } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listRules(token: string): Promise<Rule[]> {
  return apiFetch<Rule[]>(`${import.meta.env.VITE_API_BASE_URL}/api/v1/rules`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
