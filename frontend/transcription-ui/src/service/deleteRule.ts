import { apiFetch } from '../utils/apiUtils';

export async function deleteRule(ruleId: string, token: string): Promise<void> {
  await apiFetch<void>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/rules/${ruleId}`,
    {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}
