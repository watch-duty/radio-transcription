import type { Rule } from '@transcription/common';

export async function listRules(token: string): Promise<Rule[]> {
  const response = await fetch(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/rules`,
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
