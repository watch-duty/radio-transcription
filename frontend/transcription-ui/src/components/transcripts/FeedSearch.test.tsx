// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import type { Feed } from '@transcription/common';

import { FeedSearch } from './FeedSearch';

describe('FeedSearch', () => {
  const mockFeeds: Feed[] = [
    {
      id: 'zulu-feed',
      name: 'Zulu Feed',
      sourceType: 'bcfy_feeds',
      status: 'active',
    },
    {
      id: 'alpha-feed',
      name: 'Alpha Feed',
      sourceType: 'bcfy_feeds',
      status: 'active',
    },
    {
      id: 'charlie-feed',
      name: 'Charlie Feed',
      sourceType: 'bcfy_feeds',
      status: 'active',
    },
  ];

  const mockOnFeedSelect = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it('renders with a label and input', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    expect(screen.getByLabelText(/Select a registered feed/i)).toBeTruthy();
  });

  it('displays the currently selected feed in the input', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={mockFeeds[1]} // Alpha Feed
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(
      /Select a registered feed/i
    ) as HTMLInputElement;
    expect(input.value).toBe('Alpha Feed');
  });

  it('sorts options alphabetically by name when opened', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(/Select a registered feed/i);

    fireEvent.focus(input);
    fireEvent.keyDown(input, { key: 'ArrowDown' });

    const options = screen.getAllByRole('option');
    expect(options).toHaveLength(3);
    expect(options[0].textContent).toBe('Alpha Feed');
    expect(options[1].textContent).toBe('Charlie Feed');
    expect(options[2].textContent).toBe('Zulu Feed');
  });

  it('handles undefined/null feeds safely', () => {
    render(
      <FeedSearch
        feeds={null as unknown as Feed[]}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    fireEvent.focus(input);
    fireEvent.keyDown(input, { key: 'ArrowDown' });

    const options = screen.queryAllByRole('option');
    expect(options).toHaveLength(0);
  });

  it('disables the input when isFetching is true', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={true}
      />
    );

    const input = screen.getByLabelText(
      /Select a registered feed/i
    ) as HTMLInputElement;
    expect(input.disabled).toBe(true);
  });

  it('calls onFeedSelect with chosen option id on selection', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: 'Alpha Feed' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    fireEvent.keyDown(input, { key: 'Enter' });

    expect(mockOnFeedSelect).toHaveBeenCalledTimes(1);
    expect(mockOnFeedSelect).toHaveBeenCalledWith('alpha-feed');
  });

  it('filters options by name (case-insensitive)', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    fireEvent.focus(input);

    fireEvent.change(input, { target: { value: 'char' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });

    const options = screen.getAllByRole('option');
    expect(options).toHaveLength(1);
    expect(options[0].textContent).toBe('Charlie Feed');
  });

  it('filters options by id (case-insensitive)', () => {
    render(
      <FeedSearch
        feeds={mockFeeds}
        selectedFeed={null}
        onFeedSelect={mockOnFeedSelect}
        isFetching={false}
      />
    );

    const input = screen.getByLabelText(/Select a registered feed/i);
    fireEvent.focus(input);

    fireEvent.change(input, { target: { value: 'alpha-f' } });
    fireEvent.keyDown(input, { key: 'ArrowDown' });

    const options = screen.getAllByRole('option');
    expect(options).toHaveLength(1);
    expect(options[0].textContent).toBe('Alpha Feed');
  });
});
