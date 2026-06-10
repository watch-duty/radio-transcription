// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, screen } from '@testing-library/react';
import type { Feed } from '@transcription/common';
import { SourceType } from '@transcription/common';

import { renderWithRouter } from '../../test/testUtils';
import FeedHeader from './FeedHeader';

vi.mock('../feeds/FeedSearchView', () => ({
  default: vi.fn(({ onFeedSelect }) => (
    <div
      data-testid="mock-feed-search-view"
      onClick={() => onFeedSelect?.('feed123')}
    />
  )),
  FeedSearchView: vi.fn(({ onFeedSelect }) => (
    <div
      data-testid="mock-feed-search-view"
      onClick={() => onFeedSelect?.('feed123')}
    />
  )),
}));

describe('FeedHeader', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockOnError = vi.fn();
  const mockFeed: Feed = {
    id: 'feed123',
    name: 'Test Scanner Feed',
    status: 'active',
    substatus: 'active',
    lastHeartbeat: new Date().toISOString(),
    sourceUrl: 'https://test.example/source',
    archiveUrl: 'https://test.example/archives',
    sourceType: SourceType.BCFY_CALLS,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal('navigator', {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  afterEach(() => {
    cleanup();
  });

  it('renders feed name and status when available', () => {
    renderWithRouter(
      <FeedHeader
        searchedFeed={mockFeed}
        onSelectFeed={vi.fn()}
        status="active"
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    expect(screen.getAllByText('Test Scanner Feed')[0]).toBeTruthy();
    expect(screen.getByText('Active')).toBeTruthy();
  });

  it('displays the source type chip in the header', () => {
    renderWithRouter(
      <FeedHeader
        searchedFeed={mockFeed}
        onSelectFeed={vi.fn()}
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    expect(screen.getByText('bcfy_calls')).toBeTruthy();
  });

  it('shows source url link when sourceUrl is available', () => {
    renderWithRouter(
      <FeedHeader
        searchedFeed={mockFeed}
        onSelectFeed={vi.fn()}
        sourceUrl="https://test.example/source"
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    const link = screen.getByText(/original source link/i);
    expect(link).toBeTruthy();
    expect(link.getAttribute('href')).toBe('https://test.example/source');
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
  });

  it('shows archive url link when archiveUrl is available', () => {
    renderWithRouter(
      <FeedHeader
        searchedFeed={mockFeed}
        onSelectFeed={vi.fn()}
        archiveUrl="https://test.example/archives"
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    const link = screen.getByText(/archives/i);
    expect(link).toBeTruthy();
    expect(link.getAttribute('href')).toBe('https://test.example/archives');
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
  });

  it('does not render links when neither sourceUrl nor archiveUrl is supplied', () => {
    renderWithRouter(
      <FeedHeader
        searchedFeed={{
          ...mockFeed,
          sourceUrl: undefined,
          archiveUrl: undefined,
        }}
        onSelectFeed={vi.fn()}
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    expect(screen.queryByText(/original source link/i)).toBeNull();
    expect(screen.queryByText(/archives/i)).toBeNull();
  });

  it('handles Share feed link click event', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    renderWithRouter(
      <FeedHeader
        searchedFeed={mockFeed}
        onSelectFeed={vi.fn()}
        triggerSnackbar={mockTriggerSnackbar}
        onError={mockOnError}
      />
    );

    const shareButton = screen.getByRole('button', {
      name: 'copy feed deeplink',
    });
    expect(shareButton).toBeTruthy();

    fireEvent.click(shareButton);

    expect(writeTextMock).toHaveBeenCalledTimes(1);
    expect(writeTextMock).toHaveBeenCalledWith(
      expect.stringContaining('feedId=feed123')
    );
    expect(mockTriggerSnackbar).toHaveBeenCalledWith('Feed link copied');
  });

  it('displays the human-friendly relative time string for heartbeat', () => {
    const fixedNow = new Date('2026-04-10T12:05:00Z');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(fixedNow);

    try {
      renderWithRouter(
        <FeedHeader
          searchedFeed={mockFeed}
          onSelectFeed={vi.fn()}
          status="active"
          lastHeartbeat={new Date(
            fixedNow.getTime() - 5 * 60 * 1000
          ).toISOString()}
          triggerSnackbar={mockTriggerSnackbar}
          onError={mockOnError}
        />
      );

      expect(screen.getByText('Active')).toBeTruthy();
      expect(screen.getByText('Last updated: 5 minutes ago')).toBeTruthy();
    } finally {
      vi.useRealTimers();
    }
  });
});
