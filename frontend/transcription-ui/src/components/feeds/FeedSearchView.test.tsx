// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { fireEvent, screen, waitFor } from '@testing-library/react';
import { type Feed, SourceType } from '@transcription/common';

import { listFeeds } from '../../service/listFeeds';
import { renderWithQueryClient } from '../../test/testUtils';
import FeedSearchView from './FeedSearchView';

// Mock API services
vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

// Mock AuthContext
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-jwt-token-xyz' }),
}));

describe('FeedSearchView Condensed Mode', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockOnError = vi.fn();

  const mockFeeds: Feed[] = [
    {
      id: 'feed-1',
      name: 'Marin Fire Dispatch',
      sourceType: SourceType.BCFY_FEEDS,
      sourceFeedId: '33156',
      status: 'error',
      substatus: 'quarantined',
      statusReason: 'system_unexpected_error',
      quarantineReason: 'unsupported audio format',
      lastHeartbeat: new Date().toISOString(),
      tags: [{ key: 'county', value: 'Marin' }],
    },
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    mockTriggerSnackbar.mockClear();
    mockOnError.mockClear();

    // Default mock for listing feeds
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: mockFeeds,
      total: mockFeeds.length,
    });
  });

  it('passes statusReason and quarantineReason to FeedStatusIndicator in condensed search', async () => {
    renderWithQueryClient(
      <FeedSearchView
        title="Search Feeds"
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
        condensed={true}
      />
    );

    // Find the Autocomplete input and focus/click it to open options
    const input = screen.getByLabelText('Select feed');
    fireEvent.mouseDown(input);

    // Wait for option to render and be visible
    const option = await screen.findByRole('option');
    expect(option).toHaveTextContent('Marin Fire Dispatch');

    // Find the status indicator text "Error"
    const statusText = screen.getByText('Error');
    expect(statusText).toBeInTheDocument();

    // Hover over the status indicator to trigger the tooltip
    fireEvent.mouseOver(statusText);

    // Wait for the tooltip content that formats statusReason and quarantineReason
    await waitFor(() => {
      expect(
        screen.getByText('Quarantined (System Unexpected Error): unsupported audio format')
      ).toBeInTheDocument();
    });
  });
});
