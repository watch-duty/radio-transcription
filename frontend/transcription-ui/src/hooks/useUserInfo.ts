import { useQuery } from '@tanstack/react-query';
import type { UserResponse } from '@transcription/common';

import { getUserInfo } from '../service/getUserInfo';

export function useUserInfo(token: string | null) {
  return useQuery<UserResponse, Error>({
    queryKey: ['userInfo', token],
    queryFn: () => getUserInfo(token!),
    enabled: !!token,
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
  });
}
