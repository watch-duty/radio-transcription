// @vitest-environment jsdom
import React from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Feed, FeedStatus } from '@transcription/common';
import { SourceType } from '@transcription/common';


import { listFeeds } from '../../service/listFeeds';
import { FeedTable } from './FeedTable';

// Mock API services
vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

// Mock AuthContext
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-jwt-token-xyz' }),
}));

describe('FeedTable', () => {
  const mockOnError = vi.fn();

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

  beforeEach(() => {
    vi.resetAllMocks();
    mockOnError.mockClear();

    // Default mock implementation of listFeeds that mimics the filtering
    vi.mocked(listFeeds).mockImplementation(async (_token, params) => {
      let filtered = [...mockFeeds];
      if (params?.sourceTypes && params.sourceTypes.length > 0) {
        filtered = filtered.filter((f) =>
          params.sourceTypes!.includes(f.sourceType)
        );
      }
      if (params?.statuses && params.statuses.length > 0) {
        filtered = filtered.filter((f) => {
          const capitalized = f.status.charAt(0).toUpperCase() + f.status.slice(1);
          return params.statuses!.includes(capitalized.toLowerCase() as FeedStatus);
        });
      }
      if (params?.tags && params.tags.length > 0) {
        filtered = filtered.filter((f) => {
          return params.tags!.every((appliedTag) =>
            f.tags?.some(
              (tag) =>
                tag.key === appliedTag.key && tag.value === appliedTag.value
            )
          );
        });
      }
      if (params?.name) {
        const query = params.name.toLowerCase().trim();
        filtered = filtered.filter((f) => {
          const nameMatches = f.name.toLowerCase().includes(query);
          const tagMatches =
            f.tags?.some(
              (tag) =>
                tag.key.toLowerCase().includes(query) ||
                tag.value.toLowerCase().includes(query)
            ) ?? false;
          return nameMatches || tagMatches;
        });
      }
      return { feeds: filtered };
    });
  });

  afterEach(() => {
    cleanup();
    if (mockOnError.mock.calls.length > 0) {
      console.log('mockOnError calls:', JSON.stringify(mockOnError.mock.calls));
    }
  });

  const renderFeedTable = (props: Partial<React.ComponentProps<typeof FeedTable>> = {}) => {
    const testQueryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    const utils = render(
      <QueryClientProvider client={testQueryClient}>
        <MemoryRouter>
          <VirtuosoMockContext.Provider
            value={{ viewportHeight: 1000, itemHeight: 100 }}
          >
            <FeedTable onError={mockOnError} {...props} />
          </VirtuosoMockContext.Provider>
        </MemoryRouter>
      </QueryClientProvider>
    );

    return {
      ...utils,
      queryClient: testQueryClient,
      rerender: (newProps: Partial<React.ComponentProps<typeof FeedTable>> = {}) =>
        utils.rerender(
          <QueryClientProvider client={testQueryClient}>
            <MemoryRouter>
              <VirtuosoMockContext.Provider
                value={{ viewportHeight: 1000, itemHeight: 100 }}
              >
                <FeedTable onError={mockOnError} {...newProps} />
              </VirtuosoMockContext.Provider>
            </MemoryRouter>
          </QueryClientProvider>
        ),
    };
  };

  it('renders feeds and columns properly', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.getByText('bcfy_feeds')).toBeTruthy();
    expect(screen.getByText('openmhz')).toBeTruthy();

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

  it('aligns cell contents with the correct headers (Name, Type, Status)', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

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
    expect(rowCells[1].textContent).toContain('bcfy_feeds'); // Type chip
    expect(rowCells[2].textContent).toContain('Active'); // Status indicator
  });

  it('shows loading indicator when isLoading is true', async () => {
    // Return a promise that never resolves to simulate loading
    vi.mocked(listFeeds).mockReturnValue(new Promise(() => {}));
    renderFeedTable();
    expect(await screen.findByRole('progressbar')).toBeTruthy();
  });

  it('filters feeds by search bar input (name match)', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    await waitFor(() => {
      expect(screen.getByText('Bravo Scanner')).toBeTruthy();
      expect(screen.queryByText('Alpha Radio')).toBeNull();
    });
  });

  it('filters feeds by search bar input (tag match)', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'marin' } });

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeTruthy();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });
  });

  it('sorts feeds by name clicking the header sort label', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const nameHeader = screen.getByRole('columnheader', { name: /name/i });
    const nameSortLabel = within(nameHeader).getByRole('button');

    fireEvent.click(nameSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Bravo Scanner');
  });

  it('sorts feeds by status clicking the header sort label', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const statusHeader = screen.getByRole('columnheader', { name: /status/i });
    const statusSortLabel = within(statusHeader).getByRole('button');

    fireEvent.click(statusSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Alpha Radio');
  });

  it('preserves virtualized scroller position on feed refresh rerender', async () => {
    const { container, rerender, queryClient } = renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

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

    vi.mocked(listFeeds).mockResolvedValue({ feeds: refreshedFeeds });

    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ['listFeeds'] });
    });

    rerender();

    await screen.findByText('Alpha Radio (updated)');

    const refreshedScroller = container.querySelector(
      '[data-testid="virtuoso-scroller"]'
    );
    expect(refreshedScroller).toBe(scroller);
    expect(refreshedScroller?.scrollTop).toBe(200);
  });

  it('renders source and archive links directly in the links column if they exist', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

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

  it('does not render source and archive links if they are not present, and renders a fallback hyphen', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

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

  it('displays grouped tags and applies tag filtering', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const tagsInput = screen.getByLabelText('Tags');
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });

    const listbox = screen.getByRole('listbox');

    expect(within(listbox).getByText('County')).toBeTruthy();
    expect(within(listbox).getByText('Agency')).toBeTruthy();

    const countyOption = within(listbox).getByText('Marin');
    const agencyOption = within(listbox).getByText('Fire');
    expect(countyOption).toBeTruthy();
    expect(agencyOption).toBeTruthy();

    fireEvent.click(countyOption);

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeInTheDocument();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });

    expect(screen.getByText('1 Feeds on Page')).toBeTruthy();

    // Now click the agencyOption (after opening menu again)
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });
    const listbox2 = screen.getByRole('listbox');
    const agencyOption2 = within(listbox2).getByText('Fire');
    fireEvent.click(agencyOption2);

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeInTheDocument();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });

    // Click countyOption to remove it
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });
    const listbox3 = screen.getByRole('listbox');
    const countyOptionRemove = within(listbox3).getByText('Marin');
    fireEvent.click(countyOptionRemove);

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeInTheDocument();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });

    const clearButton = within(tagsInput.parentElement!).getByRole('button', {
      name: 'Clear',
    });
    fireEvent.click(clearButton);

    await waitFor(() => {
      expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    });
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('2 Feeds on Page')).toBeTruthy();
  });

  it('filters feeds by status', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const statusInput = screen.getByLabelText('Status');
    fireEvent.focus(statusInput);
    fireEvent.keyDown(statusInput, { key: 'ArrowDown' });

    const activeOption = screen.getByRole('option', { name: 'Active' });
    fireEvent.click(activeOption);

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeInTheDocument();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });
  });

  it('filters feeds by source type', async () => {
    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const sourceTypesInput = screen.getByLabelText('Source Type');
    fireEvent.focus(sourceTypesInput);
    fireEvent.keyDown(sourceTypesInput, { key: 'ArrowDown' });

    const bcfyOption = screen.getByRole('option', { name: 'bcfy_feeds' });
    fireEvent.click(bcfyOption);

    await waitFor(() => {
      expect(screen.getByText('Alpha Radio')).toBeInTheDocument();
      expect(screen.queryByText('Bravo Scanner')).toBeNull();
    });
  });

  it('does not duplicate group headers for tags in the dropdown', async () => {
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

    vi.mocked(listFeeds).mockResolvedValue({ feeds: feedsWithInterleavedTags });

    renderFeedTable();

    expect(await screen.findByText('Alpha Radio')).toBeTruthy();

    const tagsInput = screen.getByLabelText('Tags');
    fireEvent.focus(tagsInput);
    fireEvent.keyDown(tagsInput, { key: 'ArrowDown' });

    const listbox = screen.getByRole('listbox');

    // There should be exactly one 'County' group header
    const countyHeaders = within(listbox).getAllByText('County');
    expect(countyHeaders).toHaveLength(1);
  });
});
