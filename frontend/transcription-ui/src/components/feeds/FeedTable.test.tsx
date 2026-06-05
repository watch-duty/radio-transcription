// @vitest-environment jsdom
import React from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { Feed } from '@transcription/common';
import { SourceType } from '@transcription/common';

import { FeedTable } from './FeedTable';

const renderFeedTable = (props: React.ComponentProps<typeof FeedTable>) => {
  return render(
    <MemoryRouter>
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 1000, itemHeight: 100 }}
      >
        <FeedTable {...props} />
      </VirtuosoMockContext.Provider>
    </MemoryRouter>
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
    expect(rowCells[1].textContent).toContain('bcfy_feeds'); // Type chip
    expect(rowCells[2].textContent).toContain('Active'); // Status indicator
  });

  it('shows loading indicator when isLoading is true', () => {
    renderFeedTable({ feeds: [], isLoading: true });
    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('calls onFiltersChange when search bar input is updated (name match)', () => {
    vi.useFakeTimers();
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    vi.advanceTimersByTime(300);

    expect(onFiltersChangeMock).toHaveBeenCalledWith(
      expect.objectContaining({ searchQuery: 'bravo' })
    );
    vi.useRealTimers();
  });

  it('calls onFiltersChange when search bar input is updated (tag match)', () => {
    vi.useFakeTimers();
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'marin' } });

    vi.advanceTimersByTime(300);

    expect(onFiltersChangeMock).toHaveBeenCalledWith(
      expect.objectContaining({ searchQuery: 'marin' })
    );
    vi.useRealTimers();
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

    rerender(
      <MemoryRouter>
        <VirtuosoMockContext.Provider
          value={{ viewportHeight: 1000, itemHeight: 100 }}
        >
          <FeedTable feeds={refreshedFeeds} isLoading={false} />
        </VirtuosoMockContext.Provider>
      </MemoryRouter>
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

  it('displays grouped tags and calls onFiltersChange on selection', () => {
    vi.useFakeTimers();
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

    const countyOption = within(listbox).getByText('Marin');
    fireEvent.click(countyOption);

    vi.advanceTimersByTime(300);

    expect(onFiltersChangeMock).toHaveBeenCalledWith(
      expect.objectContaining({
        tags: [{ key: 'County', value: 'Marin' }],
      })
    );
    vi.useRealTimers();
  });

  it('calls onFiltersChange when status filter is selected', () => {
    vi.useFakeTimers();
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const statusInput = screen.getByLabelText('Status');
    fireEvent.focus(statusInput);
    fireEvent.keyDown(statusInput, { key: 'ArrowDown' });

    const activeOption = screen.getByRole('option', { name: 'Active' });
    fireEvent.click(activeOption);

    vi.advanceTimersByTime(300);

    expect(onFiltersChangeMock).toHaveBeenCalledWith(
      expect.objectContaining({
        statuses: ['Active'],
      })
    );
    vi.useRealTimers();
  });

  it('calls onFiltersChange when source type filter is selected', () => {
    vi.useFakeTimers();
    const onFiltersChangeMock = vi.fn();
    renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
      onFiltersChange: onFiltersChangeMock,
    });

    const sourceTypesInput = screen.getByLabelText('Source Type');
    fireEvent.focus(sourceTypesInput);
    fireEvent.keyDown(sourceTypesInput, { key: 'ArrowDown' });

    const bcfyOption = screen.getByRole('option', { name: 'bcfy_feeds' });
    fireEvent.click(bcfyOption);

    vi.advanceTimersByTime(300);

    expect(onFiltersChangeMock).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceTypes: ['bcfy_feeds'],
      })
    );
    vi.useRealTimers();
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
});
