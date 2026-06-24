// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';

import RelativeTimeDisplay from './RelativeTimeDisplay';

afterEach(() => {
  cleanup();
});

describe('RelativeTimeDisplay', () => {
  it('renders label with N/A when timestamp is not provided', () => {
    render(<RelativeTimeDisplay label="Latest" />);
    expect(screen.getByText('Latest: N/A')).toBeTruthy();
  });

  it('renders label with relative time when timestamp is provided', () => {
    const fixedNow = new Date('2026-06-09T13:00:00Z');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(fixedNow);
    try {
      render(
        <RelativeTimeDisplay
          label="Latest"
          timestamp={fixedNow.getTime() - 5 * 60 * 1000}
        />
      );
      expect(screen.getByText('Latest: 5 minutes ago')).toBeTruthy();
    } finally {
      vi.useRealTimers();
    }
  });
});
