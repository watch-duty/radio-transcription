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
  waitFor,
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
    expect(screen.getByText('-')).toBeTruthy(); // Verify empty tags render as "-"
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

    it('opens the filter popover, displays grouped tags, and applies filter', async () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false });

    // 1. Click the Filter button to open the popover
    const filterButton = screen.getByRole('button', { name: /filter/i });
    fireEvent.click(filterButton);

    // 2. Click the Open popup indicator button of Autocomplete to trigger options dropdown
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[2]);

    // Get the Autocomplete dropdown listbox
    const listbox = screen.getByRole('listbox');

    // 3. Verify that the grouped headers are rendered in the dropdown
    expect(within(listbox).getByText('County')).toBeTruthy();
    expect(within(listbox).getByText('Agency')).toBeTruthy();

    // 4. Verify the options themselves are rendered under groups in the dropdown
    const countyOption = within(listbox).getByText('Marin');
    const agencyOption = within(listbox).getByText('Fire');
    expect(countyOption).toBeTruthy();
    expect(agencyOption).toBeTruthy();

    // 5. Select the 'Marin' tag option
    fireEvent.click(countyOption);

    // 6. Click the 'Apply' button
    const applyButton = screen.getByRole('button', { name: /apply/i });
    fireEvent.click(applyButton);

    // 7. Verify the feed table is filtered to only show Alpha Radio (since it has Marin tag)
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();

    // 8. Check that the filters badge count is 1
    const badge = screen.getByText('1');
    expect(badge).toBeTruthy();

    // 9. Re-open the filter
    fireEvent.click(filterButton);

    // 10. Click the Open popup indicator button of Autocomplete to trigger options dropdown
    const openButtons2 = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons2[2]);
    const openListbox = screen.getByRole('listbox');
    const fireOption = within(openListbox).getByText('Fire');
    fireEvent.click(fireOption);

    // 11. Click the 'Cancel' button
    const cancelButton = screen.getByRole('button', { name: /cancel/i });
    fireEvent.click(cancelButton);

    // Wait for Popover to close (role presentation is removed)
    await waitFor(() => expect(screen.queryByRole('presentation')).toBeNull());

    // 12. Verify the feed table is STILL filtered ONLY to Alpha Radio (the 'Fire' selection was cancelled)
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
    expect(screen.getByText('1')).toBeTruthy(); // Badge still 1

    // 13. Re-open and check that 'Fire' is NOT selected in the input
    fireEvent.click(filterButton);
    // The chip for Fire should not be present, but Marin should be
    const popover = screen.getByRole('presentation');
    expect(within(popover).getByText('County')).toBeTruthy();
    expect(within(popover).getByText(': Marin')).toBeTruthy();
    expect(within(popover).queryByText('Agency')).toBeNull();

    // 14. Clear the filters
    const clearButton = screen.getByRole('button', { name: /clear/i });
    fireEvent.click(clearButton);

    // 15. Verify both feeds are displayed again
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
  });

  it('renders the filter inline and applies filtering immediately when collapse=false', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, collapse: false });

    // 1. Verify that the "Filters" popover button is NOT present
    expect(screen.queryByRole('button', { name: /filter/i })).toBeNull();

    // 2. Verify that the Autocomplete input is present and open it
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[2]);

    // 3. Select the 'Marin' tag option
    const listbox = screen.getByRole('listbox');
    const countyOption = within(listbox).getByText('Marin');
    fireEvent.click(countyOption);

    // 4. Verify the feed table is filtered immediately (no 'Apply' button needed)
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('filters feeds by status when rendered inline (collapse=false)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, collapse: false });

    // Find the Status autocomplete and open it. We get the open button for the Status autocomplete (index 1).
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[1]);

    // Click the 'Active' status option
    const activeOption = screen.getByRole('option', { name: 'Active' });
    fireEvent.click(activeOption);

    // Alpha Radio (Active) should be present, Bravo Scanner (Inactive) should be filtered out
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('filters feeds by status in the popover (collapse=true)', async () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, collapse: true });

    // Open popover
    const filterButton = screen.getByRole('button', { name: /filter/i });
    fireEvent.click(filterButton);

    // Click the Open button of the Status Autocomplete inside the popover.
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[1]);

    // Click 'Inactive' status option
    const inactiveOption = screen.getByRole('option', { name: 'Inactive' });
    fireEvent.click(inactiveOption);

    // Click Apply
    const applyButton = screen.getByRole('button', { name: /apply/i });
    fireEvent.click(applyButton);

    // Alpha Radio should be filtered out, Bravo Scanner should be present
    expect(screen.queryByText('Alpha Radio')).toBeNull();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
  });

  it('filters feeds by source type when rendered inline (collapse=false)', () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, collapse: false });

    // Open the dropdown for the Source Type autocomplete (index 0)
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[0]);

    // Click the 'bcfy_feeds' option
    const bcfyOption = screen.getByRole('option', { name: 'bcfy_feeds' });
    fireEvent.click(bcfyOption);

    // Alpha Radio (bcfy_feeds) should be present, Bravo Scanner (openmhz) should be filtered out
    expect(screen.getByText('Alpha Radio')).toBeTruthy();
    expect(screen.queryByText('Bravo Scanner')).toBeNull();
  });

  it('filters feeds by source type in the popover (collapse=true)', async () => {
    renderFeedTable({ feeds: mockFeeds, isLoading: false, collapse: true });

    // Open popover
    const filterButton = screen.getByRole('button', { name: /filter/i });
    fireEvent.click(filterButton);

    // Click the Open button of the Source Type Autocomplete inside the popover (index 0).
    const openButtons = screen.getAllByRole('button', { name: 'Open' });
    fireEvent.click(openButtons[0]);

    // Click 'openmhz' option
    const openmhzOption = screen.getByRole('option', { name: 'openmhz' });
    fireEvent.click(openmhzOption);

    // Click Apply
    const applyButton = screen.getByRole('button', { name: /apply/i });
    fireEvent.click(applyButton);

    // Alpha Radio should be filtered out, Bravo Scanner should be present
    expect(screen.queryByText('Alpha Radio')).toBeNull();
    expect(screen.getByText('Bravo Scanner')).toBeTruthy();
  });
});
