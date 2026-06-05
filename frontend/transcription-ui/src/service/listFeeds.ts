import type { Feed, ListFeedsResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export interface ListFeedsParams {
  limit?: number;
  nextToken?: string;
  sourceTypes?: string[];
  statuses?: string[];
  tags?: { key: string; value: string }[];
  name?: string;
}

export async function listFeeds(
  token: string,
  params?: ListFeedsParams
): Promise<ListFeedsResponse> {
  const queryParams = new URLSearchParams();
  if (params?.limit) queryParams.append('limit', params.limit.toString());
  if (params?.nextToken) queryParams.append('nextToken', params.nextToken);
  if (params?.name) queryParams.append('name', params.name);
  if (params?.sourceTypes && params.sourceTypes.length > 0) {
    queryParams.append('sourceTypes', params.sourceTypes.join(','));
  }
  if (params?.statuses && params.statuses.length > 0) {
    queryParams.append(
      'statuses',
      params.statuses.map((s) => s.toLowerCase()).join(',')
    );
  }
  if (params?.tags && params.tags.length > 0) {
    for (const tag of params.tags) {
      queryParams.append('tags', JSON.stringify(tag));
    }
  }

  const url = queryParams.toString()
    ? `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds?${queryParams.toString()}`
    : `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`;

  return apiFetch<ListFeedsResponse | Feed[]>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  }).then((resp) => {
    if (Array.isArray(resp)) {
      return { feeds: resp };
    } else {
      return resp;
    }
  });
}
