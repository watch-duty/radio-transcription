import { apiFetch } from '../utils/apiUtils';

export async function deactivateFeed(
  feedId: string,
  token: string
): Promise<void> {
  await apiFetch<void>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}/deactivate`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}
