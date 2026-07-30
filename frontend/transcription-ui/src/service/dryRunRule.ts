import type { DryRunRequest, DryRunResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export const dryRunRule = async (
  request: DryRunRequest,
  token?: string
): Promise<DryRunResponse> => {
  return apiFetch<DryRunResponse>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/rules/dry-run`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(request),
    }
  );
};
