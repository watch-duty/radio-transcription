// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '@transcription/common';

import FeedStatusIndicator from './FeedStatusIndicator';

afterEach(() => {
  cleanup();
});

describe('FeedStatusIndicator', () => {
  it('renders null when no status is provided', () => {
    const { container } = render(<FeedStatusIndicator />);
    expect(container.firstChild).toBeNull();
  });

  it('renders active status correctly', () => {
    render(<FeedStatusIndicator status="active" />);
    expect(screen.getByText('Active')).toBeTruthy();
  });

  it('renders inactive status correctly', () => {
    render(<FeedStatusIndicator status="inactive" />);
    expect(screen.getByText('Inactive')).toBeTruthy();
  });

  it('renders error status correctly', () => {
    render(<FeedStatusIndicator status="error" />);
    expect(screen.getByText('Error')).toBeTruthy();
  });

  it('renders custom status correctly', () => {
    render(
      <FeedStatusIndicator status={'unknown_status' as unknown as FeedStatus} />
    );
    expect(screen.getByText('unknown_status')).toBeTruthy();
  });

  it('displays substatus tooltip on hover', async () => {
    render(<FeedStatusIndicator status="active" substatus="active" />);
    const statusText = screen.getByText('Active');
    fireEvent.mouseOver(statusText);
    await waitFor(() => {
      expect(screen.getByText('Active')).toBeTruthy();
    });
  });

  it('displays substatus and statusReason in tooltip on hover', async () => {
    render(
      <FeedStatusIndicator
        status="error"
        substatus="failing"
        statusReason="source_offline"
      />
    );
    const statusText = screen.getByText('Error');
    fireEvent.mouseOver(statusText);
    await waitFor(() => {
      expect(screen.getByText('Failing (Source Offline)')).toBeTruthy();
    });
  });

  it('displays custom substatus and reason in tooltip on hover', async () => {
    render(
      <FeedStatusIndicator
        status="error"
        substatus={'custom_substatus' as unknown as BackendFeedStatus}
        statusReason={'custom_reason' as unknown as BackendFeedStatusReason}
      />
    );
    const statusText = screen.getByText('Error');
    fireEvent.mouseOver(statusText);
    await waitFor(() => {
      expect(screen.getByText('custom_substatus (custom_reason)')).toBeTruthy();
    });
  });

  it('displays quarantineReason in tooltip on hover', async () => {
    render(
      <FeedStatusIndicator
        status="error"
        substatus="quarantined"
        quarantineReason="corrupted audio files"
      />
    );
    const statusText = screen.getByText('Error');
    fireEvent.mouseOver(statusText);
    await waitFor(() => {
      expect(
        screen.getByText('Quarantined: corrupted audio files')
      ).toBeTruthy();
    });
  });

  it('displays lastHeartbeat relative time string', () => {
    const fixedNow = new Date('2026-06-09T13:00:00Z');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(fixedNow);
    try {
      render(
        <FeedStatusIndicator
          status="active"
          lastHeartbeat={new Date(
            fixedNow.getTime() - 10 * 60 * 1000
          ).toISOString()}
        />
      );
      expect(screen.getByText('Last updated: 10 minutes ago')).toBeTruthy();
    } finally {
      vi.useRealTimers();
    }
  });
});
