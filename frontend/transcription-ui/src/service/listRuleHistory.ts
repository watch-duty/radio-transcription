import type { PaginatedRuleAuditEvents } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listRuleHistory(
  ruleId: string,
  token: string,
  limit?: number,
  nextToken?: string
): Promise<PaginatedRuleAuditEvents> {
  const queryParams = new URLSearchParams();
  if (limit !== undefined) {
    queryParams.append('limit', limit.toString());
  }
  if (nextToken) {
    queryParams.append('nextToken', nextToken);
  }
  const queryString = queryParams.toString();
  const url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/rules/${ruleId}/history${queryString ? `?${queryString}` : ''}`;
  return apiFetch<PaginatedRuleAuditEvents>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
