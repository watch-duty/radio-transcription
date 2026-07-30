// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { fireEvent, screen, waitFor } from '@testing-library/react';
import { type Feed, SourceType } from '@transcription/common';

import { getFeedSearchOptions } from '../../service/getFeedSearchOptions';
import { listFeeds } from '../../service/listFeeds';
import { renderWithQueryClient } from '../../test/testUtils';
import FeedSearchView from './FeedSearchView';

// Mock API services
vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

vi.mock('../../service/getFeedSearchOptions', () => ({
  getFeedSearchOptions: vi.fn(),
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
      statusReasonDetail: 'unsupported audio format',
      lastHeartbeat: new Date().getTime(),
      lastSpeechSegmentTimestamp: new Date().getTime(),
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
    vi.mocked(getFeedSearchOptions).mockResolvedValue({
      sourceTypes: [SourceType.BCFY_FEEDS],
      statuses: [],
      tags: [],
    });
  });

  it('passes statusReason and statusReasonDetail to FeedStatusIndicator in condensed search', async () => {
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

    // Wait for the tooltip content that formats statusReason and statusReasonDetail
    await waitFor(() => {
      expect(
        screen.getByText(
          'Quarantined (System Unexpected Error): unsupported audio format'
        )
      ).toBeInTheDocument();
    });
  });
});

describe('FeedSearchView status filter', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockOnError = vi.fn();

  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(listFeeds).mockResolvedValue({ feeds: [], total: 0 });
    // Raw backend statuses, as /feeds/search-options returns them.
    vi.mocked(getFeedSearchOptions).mockResolvedValue({
      sourceTypes: [SourceType.BCFY_FEEDS],
      statuses: [
        'active',
        'deactivated',
        'failing',
        'quarantined',
        'unclaimed',
      ],
      tags: [],
    });
  });

  it('offers normalized bucket options and filters by the selection', async () => {
    renderWithQueryClient(
      <FeedSearchView
        title="Feeds"
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    const statusInput = await screen.findByLabelText('Status');
    fireEvent.focus(statusInput);
    fireEvent.keyDown(statusInput, { key: 'ArrowDown' });

    // The dropdown offers the deduplicated presentation buckets, not
    // the raw backend statuses.
    await waitFor(() => {
      expect(document.querySelector('[data-value="active"]')).toBeTruthy();
    });
    const optionValues = Array.from(
      document.querySelectorAll('li[data-value]')
    ).map((option) => option.getAttribute('data-value'));
    expect(optionValues).toEqual(['active', 'inactive', 'error']);

    fireEvent.click(
      document.querySelector('[data-value="inactive"]') as HTMLElement
    );

    // A non-active selection reaches the list request as a real status
    // filter (listFeeds expands it to the raw backend statuses).
    await waitFor(() => {
      expect(listFeeds).toHaveBeenCalledWith(
        'fake-jwt-token-xyz',
        expect.objectContaining({ statuses: ['inactive'] })
      );
    });
  });
});
