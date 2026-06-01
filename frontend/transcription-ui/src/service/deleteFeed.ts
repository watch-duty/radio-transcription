import { apiFetch } from '../utils/apiUtils';

export async function deleteFeed(feedId: string, token: string): Promise<void> {
  await apiFetch<void>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}`,
    {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}
