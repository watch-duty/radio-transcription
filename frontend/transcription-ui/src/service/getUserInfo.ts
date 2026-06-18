import type { UserResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function getUserInfo(token: string): Promise<UserResponse> {
  return apiFetch<UserResponse>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/users`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}
