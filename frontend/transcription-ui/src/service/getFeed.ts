import type { Feed } from '@transcription/common';

export async function getFeed(feedId: string, token: string): Promise<Feed> {
  const response = await fetch(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error(`Error: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  return data;
}
