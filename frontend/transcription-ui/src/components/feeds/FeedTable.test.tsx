// @vitest-environment jsdom
import React from 'react';
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

import { afterEach, describe, expect, it } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { Feed } from '@transcription/common';

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
      sourceType: 'bcfy_feeds',
      status: 'active',
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
      sourceType: 'openmhz',
      status: 'inactive',
      // Missing links to test disabled items
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

    // Status check (actual text is capitalized, rendered uppercase via CSS)
    expect(screen.getByText('Active')).toBeTruthy();
    expect(screen.getByText('Inactive')).toBeTruthy();

    // Tags check
    expect(screen.getByText(/Marin/i)).toBeTruthy();
    expect(screen.getByText(/Fire/i)).toBeTruthy();
  });

  it('shows loading indicator when isLoading is true', () => {
    renderFeedTable({ feeds: [], isLoading: true });
    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('filters feeds by search bar input (name match)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.queryByText('Alpha Radio')).toBeNull();
  });

  it('filters feeds by search bar input (tag match)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'marin' } });

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('sorts feeds by name clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const nameSortLabel = screen.getByText('Name');

    // Initially asc, click to sort desc
    fireEvent.click(nameSortLabel);

    const cells = screen.getAllByRole('cell');
    // The first body cell should contain Bravo Scanner in desc order
    expect(cells[0].textContent).toContain('Bravo Scanner');
  });

  it('sorts feeds by status clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const statusSortLabel = screen.getByText('Status');

    // Click to sort by status
    fireEvent.click(statusSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Alpha Radio');
  });

  it('preserves virtualized scroller position on feed refresh rerender', () => {
    const { container, rerender } = renderFeedTable({
      feeds: mockFeeds,
      isLoading: false,
    });
    const scroller = container.querySelector('[data-testid="virtuoso-scroller"]');
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

  it('opens the three dot menu and disables links if not present', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const actionButtons = screen.getAllByRole('button', {
      name: /feed actions/i,
    });

    // Click actions for Bravo Scanner (index 1) which has no links
    fireEvent.click(actionButtons[1]);

    const menu = screen.getByRole('menu');
    expect(menu).toBeTruthy();

    const sourceUrlItem = within(menu).getByText('Source URL').closest('li');
    const archiveUrlItem = within(menu).getByText('Archive URL').closest('li');

    expect(sourceUrlItem?.getAttribute('aria-disabled')).toBe('true');
    expect(archiveUrlItem?.getAttribute('aria-disabled')).toBe('true');
  });

  it('enables links in menu if URLs are present', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    const actionButtons = screen.getAllByRole('button', {
      name: /feed actions/i,
    });

    // Click actions for Alpha Radio (index 0) which has links
    fireEvent.click(actionButtons[0]);

    const menu = screen.getByRole('menu');
    const sourceUrlItem = within(menu).getByText('Source URL').closest('a');
    const archiveUrlItem = within(menu).getByText('Archive URL').closest('a');

    expect(sourceUrlItem?.getAttribute('href')).toBe(
      'https://example.com/source'
    );
    expect(archiveUrlItem?.getAttribute('href')).toBe(
      'https://example.com/archive'
    );
    expect(sourceUrlItem?.getAttribute('target')).toBe('_blank');
  });
});
