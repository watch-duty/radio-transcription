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
    },
  ];

  afterEach(() => {
    cleanup();
  });

  it('renders feeds and columns properly', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.getByText('bcfy_feeds')).toBeTruthy();
    expect(screen.getByText('openmhz')).toBeTruthy();

    expect(screen.getByText('Active')).toBeTruthy();
    expect(screen.getByText('Inactive')).toBeTruthy();

    expect(screen.getByText(/Marin/i)).toBeTruthy();
    expect(screen.getByText(/Fire/i)).toBeTruthy();
    expect(screen.getByText('-')).toBeTruthy();
  });

  it('shows loading indicator when isLoading is true', () => {
    renderFeedTable({ feeds: [], isLoading: true, title: 'Feeds' });
    expect(screen.getByRole('progressbar')).toBeTruthy();
  });

  it('filters feeds by search bar input (name match)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'bravo' } });

    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.queryByText('Alpha Radio')).toBeNull();
  });

  it('filters feeds by search bar input (tag match)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const searchInput = screen.getByPlaceholderText(/Search feeds\.\.\./i);
    fireEvent.change(searchInput, { target: { value: 'marin' } });

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('sorts feeds by name clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const nameHeader = screen.getByRole('columnheader', { name: /name/i });
    const nameSortLabel = within(nameHeader).getByRole('button');

    fireEvent.click(nameSortLabel);

    const cells = screen.getAllByRole('cell');
    expect(cells[0].textContent).toContain('Bravo Scanner');
  });

  it('sorts feeds by status clicking the header sort label', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

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
      title: 'Feeds',
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
          <FeedTable feeds={refreshedFeeds} isLoading={false} title="Feeds" />
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
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const actionButtons = screen.getAllByRole('button', {
      name: /feed actions/i,
    });

    fireEvent.click(actionButtons[1]);

    const menu = screen.getByRole('menu');
    expect(menu).toBeTruthy();

    const sourceUrlItem = within(menu).getByText('Source URL').closest('li');
    const archiveUrlItem = within(menu).getByText('Archive URL').closest('li');

    expect(sourceUrlItem?.getAttribute('aria-disabled')).toBe('true');
    expect(archiveUrlItem?.getAttribute('aria-disabled')).toBe('true');
  });

  it('enables links in menu if URLs are present', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const actionButtons = screen.getAllByRole('button', {
      name: /feed actions/i,
    });

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

  it('displays grouped tags and applies tag filtering', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

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

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();

    expect(screen.getByText('Showing 1 of 2 feeds')).toBeTruthy();

    fireEvent.click(agencyOption);

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();

    fireEvent.click(countyOption);

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();

    const clearButton = within(tagsInput.parentElement!).getByRole('button', {
      name: 'Clear',
    });
    fireEvent.click(clearButton);

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
    expect(screen.queryByText('Showing 1 of 2 feeds')).toBeNull();
  });

  it('filters feeds by status', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const statusInput = screen.getByLabelText('Status');
    fireEvent.focus(statusInput);
    fireEvent.keyDown(statusInput, { key: 'ArrowDown' });

    const activeOption = screen.getByRole('option', { name: 'Active' });
    fireEvent.click(activeOption);

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('filters feeds by source type', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, title: 'Feeds' });

    const sourceTypesInput = screen.getByLabelText('Source Type');
    fireEvent.focus(sourceTypesInput);
    fireEvent.keyDown(sourceTypesInput, { key: 'ArrowDown' });

    const bcfyOption = screen.getByRole('option', { name: 'bcfy_feeds' });
    fireEvent.click(bcfyOption);

    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });
});
