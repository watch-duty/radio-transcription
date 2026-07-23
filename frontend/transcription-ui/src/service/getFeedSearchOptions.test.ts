import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiFetch } from '../utils/apiUtils';
import { getFeedSearchOptions } from './getFeedSearchOptions';

vi.mock('../utils/apiUtils', () => ({
  apiFetch: vi.fn(),
}));

describe('getFeedSearchOptions', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches search options with token', async () => {
    const mockResponse = {
      sourceTypes: ['bcfy_feeds'],
      statuses: ['active'],
      tags: [{ key: 'county', value: 'Marin' }],
    };
    vi.mocked(apiFetch).mockResolvedValueOnce(mockResponse);

    const result = await getFeedSearchOptions('test-token');

    expect(apiFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/search-options'),
      {
        headers: {
          Authorization: 'Bearer test-token',
        },
      }
    );
    expect(result).toEqual(mockResponse);
  });
});
