// @vitest-environment jsdom
import React from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { Feed } from '@transcription/common';
import { SourceType } from '@transcription/common';

import { listFeedHistory } from '../../service/listFeedHistory';
import { type FeedFilters, FeedTable } from './FeedTable';

vi.mock('../../service/listFeedHistory', () => ({
  listFeedHistory: vi.fn(),
}));

vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-jwt-token-xyz' }),
}));

const defaultFilters: FeedFilters = {
  searchQuery: '',
  sourceTypes: [],
  statuses: [],
  tags: [],
};

const renderFeedTable = (
  props: Partial<React.ComponentProps<typeof FeedTable>> = {}
) => {
  const feeds = props.feeds ?? [];
  const seen = new Set<string>();
  const uniqueTags: { key: string; value: string }[] = [];
  feeds.forEach((feed) => {
    feed.tags?.forEach((tag) => {
      const identifier = `${tag.key}:${tag.value}`;
      if (!seen.has(identifier)) {
        seen.add(identifier);
        uniqueTags.push({ key: tag.key, value: tag.value });
      }
    });
  });
  const sortedTags = uniqueTags.sort(
    (a, b) => a.key.localeCompare(b.key) || a.value.localeCompare(b.value)
  );

  const finalProps = {
    feeds: [],
    tags: sortedTags,
    isLoading: false,
    filters: defaultFilters,
    onFiltersChange: vi.fn(),
    feedTotal: 0,
    ...props,
  };

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <VirtuosoMockContext.Provider
          value={{ viewportHeight: 1000, itemHeight: 100 }}
        >
          <FeedTable {...finalProps} />
        </VirtuosoMockContext.Provider>
      </MemoryRouter>
    </QueryClientProvider>
  );
};

describe('FeedTable', () => {
  const mockFeeds: Feed[] = [
    {
      id: 'feed-1',
      name: 'Alpha Radio',
      sourceType: SourceType.BCFY_FEEDS,
      status: 'active',
      substatus: 'active',
      sourceUrl: 'https://example.com/source',
      archiveUrl: 'https://example.com/archive',
      tags: [
        { key: 'County', value: 'Marin' },
        { key: 'Agency', value: 'Fire' },
      ],
    },
    {
      id: 'feed-2',
      name: 'Bravo Scanner',
      sourceType: SourceType.OPENMHZ,
      status: 'inactive',
      substatus: 'deactivated',
    },
  ];

  afterEach(() => {
    cleanup();
  });

  it('renders feeds and columns properly', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.getByText('Broadcastify Feeds')).toBeTruthy();
    expect(screen.getByText('OpenMHz')).toBeTruthy();

    expect(screen.getByText('Active')).toBeTruthy();
    expect(screen.getByText('Inactive')).toBeTruthy();

    expect(screen.getByText(/Marin/i)).toBeTruthy();
    expect(screen.getByText(/Fire/i)).toBeTruthy();

    // Verify Bravo Scanner row details (no tags cell should render, and renders a fallback hyphen for links)
    const bravoRow = screen
      .getByText('Bravo Scanner')
      .closest('[role="row"]') as HTMLElement;
    expect(bravoRow).toBeTruthy();
    if (bravoRow) {
      expect(within(bravoRow).getByText('-')).toBeInTheDocument();
      const tagChips = bravoRow.querySelectorAll('.MuiChip-filled');
      expect(tagChips).toHaveLength(0);
    }
  });

  it('aligns cell contents with the correct headers (Name, Type, Status)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const headers = screen
      .getAllByRole('columnheader')
      .map((h) => h.textContent);

    // Header text check
    expect(headers[0]).toContain('Name');
    expect(headers[1]).toContain('Type');
    expect(headers[2]).toContain('Status');

    // Get the Alpha Radio row and cells
    const alphaRow = screen
      .getByText('Alpha Radio')
      .closest('[role="row"]') as HTMLElement;
    expect(alphaRow).toBeTruthy();

    const rowCells = within(alphaRow).getAllByRole('cell');

    // Cell index matching header index check
    expect(rowCells[0].textContent).toContain('Alpha Radio');
    expect(rowCells[1].textContent).toContain('Broadcastify Feeds'); // Type chip
    expect(rowCells[2].textContent).toContain('Active'); // Status indicator
  });

  it('shows loading indicator when isLoading is true', () => {
    renderFeedTable({ feeds: [], isLoading: true });
    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('triggers onFiltersChange with name match search input', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    expect(onFiltersChangeMock).toHaveBeenCalledWith({
      searchQuery: 'bravo',
      sourceTypes: [],
      statuses: [],
      tags: [],
    });
  });

  it('triggers onFiltersChange with tag match search input', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'marin' } });

    expect(onFiltersChangeMock).toHaveBeenCalledWith({
      searchQuery: 'marin',
      sourceTypes: [],
      statuses: [],
      tags: [],
    });
  });

  it('sorts feeds by name clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const nameHeader = screen.getByRole('columnheader', { name: /name/i });
    const nameSortLabel = within(nameHeader).getByRole('button');

    fireEvent.click(nameSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Bravo Scanner');
  });

  it('sorts feeds by status clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const statusHeader = screen.getByRole('columnheader', { name: /status/i });
    const statusSortLabel = within(statusHeader).getByRole('button');

    fireEvent.click(statusSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Alpha Radio');
  });

  it('preserves virtualized scroller position on feed refresh rerender', () => {
    const { container, rerender } = renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
    });
    const scroller = container.querySelector(
      '[data-testid="virtuoso-scroller"]'
    );
    expect(scroller).toBeTruthy();
    if (!scroller) {
      throw new Error('Expected virtuoso scroller to be rendered');
    }

    scroller.scrollTop = 200;
    const refreshedFeeds = mockFeeds.map((feed) => ({
      ...feed,
      name: `${feed.name} (updated)`,
    }));

    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    rerender(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <VirtuosoMockContext.Provider
            value={{ viewportHeight: 1000, itemHeight: 100 }}
          >
            <FeedTable
              feeds={refreshedFeeds}
              isLoading={false}
              filters={defaultFilters}
              onFiltersChange={vi.fn()}
              feedTotal={1}
              tags={[]}
            />
          </VirtuosoMockContext.Provider>
        </MemoryRouter>
      </QueryClientProvider>
    );

    const refreshedScroller = container.querySelector(
      '[data-testid="virtuoso-scroller"]'
    );
    expect(refreshedScroller).toBe(scroller);
    expect(refreshedScroller?.scrollTop).toBe(200);
  });

  it('renders source and archive links directly in the links column if they exist', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    // Alpha Radio has both URLs
    const sourceLink = screen.getByRole('link', {
      name: 'https://example.com/source',
    });
    const archiveLink = screen.getByRole('link', {
      name: 'https://example.com/archive',
    });

    expect(sourceLink).toBeTruthy();
    expect(sourceLink.getAttribute('href')).toBe('https://example.com/source');
    expect(sourceLink.getAttribute('target')).toBe('_blank');

    expect(archiveLink).toBeTruthy();
    expect(archiveLink.getAttribute('href')).toBe(
      'https://example.com/archive'
    );
    expect(archiveLink.getAttribute('target')).toBe('_blank');
  });

  it('does not render source and archive links if they are not present, and renders a fallback hyphen', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    // Verify Alpha Radio has its links rendered correctly
    const links = screen.queryAllByRole('link', {
      name: /https:\/\/example\.com/i,
    });
    expect(links).toHaveLength(2); // Only the 2 links of Alpha Radio exist!

    // Verify Bravo Scanner has no source/archive URLs and falls back to rendering a hyphen inside its row context
    const bravoRow = screen
      .getByText('Bravo Scanner')
      .closest('[role="row"]') as HTMLElement;
    expect(bravoRow).toBeTruthy();
    if (bravoRow) {
      const bravoSourceLinks = within(bravoRow).queryAllByRole('link', {
        name: /https:/i,
      });
      expect(bravoSourceLinks).toHaveLength(0);
      expect(within(bravoRow).getByText('-')).toBeInTheDocument();
    }
  });

  it('displays grouped tags and triggers onFiltersChange on selection', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const tagsInput = screen.getByLabelText('Tags');
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });

    const listbox = screen.getByRole('listbox');

    expect(within(listbox).getByText('County')).toBeTruthy();
    expect(within(listbox).getByText('Agency')).toBeTruthy();

    const countyOption = document.querySelector(
      '[data-value="County:Marin"]'
    ) as HTMLElement;
    const agencyOption = document.querySelector(
      '[data-value="Agency:Fire"]'
    ) as HTMLElement;
    expect(countyOption).toBeTruthy();
    expect(agencyOption).toBeTruthy();

    fireEvent.click(countyOption);

    expect(onFiltersChangeMock).toHaveBeenCalledWith({
      searchQuery: '',
      sourceTypes: [],
      statuses: [],
      tags: [{ key: 'County', value: 'Marin' }],
    });
  });

  it('triggers onFiltersChange when status filter is updated', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const statusInput = screen.getByLabelText('Status');
    fireEvent.focus(statusInput);
    fireEvent.keyDown(statusInput, { key: 'ArrowDown' });

    const activeOption = document.querySelector(
      '[data-value="Active"]'
    ) as HTMLElement;
    expect(activeOption).toBeTruthy();
    fireEvent.click(activeOption);

    expect(onFiltersChangeMock).toHaveBeenCalledWith({
      searchQuery: '',
      sourceTypes: [],
      statuses: ['Active'],
      tags: [],
    });
  });

  it('triggers onFiltersChange when source type filter is updated', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const sourceTypesInput = screen.getByLabelText('Source Type');
    fireEvent.focus(sourceTypesInput);
    fireEvent.keyDown(sourceTypesInput, { key: 'ArrowDown' });

    const bcfyOption = document.querySelector(
      '[data-value="bcfy_feeds"]'
    ) as HTMLElement;
    expect(bcfyOption).toBeTruthy();
    fireEvent.click(bcfyOption);

    expect(onFiltersChangeMock).toHaveBeenCalledWith({
      searchQuery: '',
      sourceTypes: ['bcfy_feeds'],
      statuses: [],
      tags: [],
    });
  });

  it('does not duplicate group headers for tags in the dropdown', () => {
    const feedsWithInterleavedTags: Feed[] = [
      {
        id: 'feed-1',
        name: 'Alpha Radio',
        sourceType: SourceType.BCFY_FEEDS,
        status: 'active',
        substatus: 'active',
        tags: [
          { key: 'County', value: 'Marin' },
          { key: 'Agency', value: 'Fire' },
        ],
      },
      {
        id: 'feed-2',
        name: 'Bravo Scanner',
        sourceType: SourceType.BCFY_FEEDS,
        status: 'active',
        substatus: 'active',
        tags: [{ key: 'State', value: 'CA' }],
      },
      {
        id: 'feed-3',
        name: 'Charlie Scanner',
        sourceType: SourceType.BCFY_FEEDS,
        status: 'active',
        substatus: 'active',
        tags: [{ key: 'County', value: 'Sonoma' }],
      },
    ];

    renderFeedTable({ feeds: feedsWithInterleavedTags, isLoading: false });

    const tagsInput = screen.getByLabelText('Tags');
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });

    const listbox = screen.getByRole('listbox');

    // There should be exactly one 'County' group header
    const countyHeaders = within(listbox).getAllByText('County');
    expect(countyHeaders).toHaveLength(1);
  });

  it('calls onFiltersChange when filters are updated', () => {
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    expect(onFiltersChangeMock).toHaveBeenLastCalledWith({
      searchQuery: 'bravo',
      sourceTypes: [],
      statuses: [],
      tags: [],
    });
  });

  it('does not render visibility eye icon when allowEdit is false', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, allowEdit: false });
    expect(screen.queryByLabelText(/View audit trail for/i)).toBeNull();
  });

  it('renders visibility eye icon and opens history modal on click when allowEdit is true', async () => {
    const mockHistoryEvents = [
      {
        id: 'evt_1',
        feedId: 'feed-1',
        action: 'feed.recovered',
        actor: 'system',
        occurredAt: Date.parse('2026-07-01T12:00:00Z'),
        feedRevision: 1,
        beforeValues: { status: 'failing' },
        afterValues: { status: 'active' },
      },
    ];
    vi.mocked(listFeedHistory).mockResolvedValue({
      historyEvents: mockHistoryEvents,
      total: 1,
    });

    renderFeedTable({ feeds: mockFeeds, isLoading: false, allowEdit: true });

    const viewAuditBtn = screen.getByLabelText(
      'View audit trail for Alpha Radio'
    );
    expect(viewAuditBtn).toBeInTheDocument();

    fireEvent.click(viewAuditBtn);

    // Verify listFeedHistory is called
    expect(listFeedHistory).toHaveBeenCalledWith(
      'feed-1',
      'fake-jwt-token-xyz'
    );

    // Verify modal is open and elements are rendered
    await screen.findByText('Audit Trail: Alpha Radio');
    expect(
      await screen.findByText('Wednesday, July 1, 2026')
    ).toBeInTheDocument();
    expect(screen.getByText(/Feed recovered successfully/)).toBeInTheDocument();
  });
});
