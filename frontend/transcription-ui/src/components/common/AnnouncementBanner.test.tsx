// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { AnnouncementBanner } from './AnnouncementBanner';

const TEST_START_DATE = new Date('2026-07-20T00:00:00');
const TEST_END_DATE = new Date('2026-07-29T23:59:59');
const TEST_LINK_URL = 'https://test-form';
const TEST_MESSAGE = 'Please share your experience by Wed, July 29!';

afterEach(() => {
  cleanup();
});

describe('AnnouncementBanner', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('does not render before the start date', () => {
    const beforeStart = new Date(TEST_START_DATE.getTime() - 1000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="CSAT Survey:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        currentDate={beforeStart}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('does not render after the end date', () => {
    const afterEnd = new Date(TEST_END_DATE.getTime() + 1000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="CSAT Survey:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        currentDate={afterEnd}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('renders within the valid date window with title, message, and custom link text', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="CSAT Survey:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        linkText={`${TEST_LINK_URL} (2 min survey)`}
        currentDate={duringWindow}
      />
    );
    expect(screen.getByText('CSAT Survey:')).toBeTruthy();
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();
    const link = screen.getByRole('link', {
      name: /https:\/\/forms\.gle\/KocdXk8qWXyw7UCw9 \(2 min survey\)/i,
    });
    expect(link.getAttribute('href')).toBe(TEST_LINK_URL);
  });

  it('renders correctly without title or linkUrl when only message is provided', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <AnnouncementBanner
        startDate="2026-07-20T00:00:00"
        endDate="2026-07-29T23:59:59"
        message="System maintenance scheduled for Saturday."
        currentDate={duringWindow}
      />
    );
    expect(
      screen.getByText('System maintenance scheduled for Saturday.')
    ).toBeTruthy();
    expect(screen.queryByRole('link')).toBeNull();
  });

  it('renders when forceShow is set to true even outside date window', () => {
    const beforeStart = new Date(TEST_START_DATE.getTime() - 1000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="Notice:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        currentDate={beforeStart}
        forceShow
      />
    );
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();
  });

  it('closes and saves dismissal to localStorage when close button is clicked', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="Notice:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        currentDate={duringWindow}
      />
    );
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();

    const closeButton = screen.getByRole('button', { name: /close/i });
    fireEvent.click(closeButton);

    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
    expect(
      localStorage.getItem(`announcement_banner_dismissed_${TEST_LINK_URL}`)
    ).toBe('true');
  });

  it('does not render if previously dismissed in localStorage', () => {
    localStorage.setItem(
      `announcement_banner_dismissed_${TEST_LINK_URL}`,
      'true'
    );
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="Notice:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        currentDate={duringWindow}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('uses custom storageKey when provided', () => {
    const customKey = 'my_custom_announcement_dismissal';
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <AnnouncementBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        title="Notice:"
        message={TEST_MESSAGE}
        linkUrl={TEST_LINK_URL}
        storageKey={customKey}
        currentDate={duringWindow}
      />
    );

    const closeButton = screen.getByRole('button', { name: /close/i });
    fireEvent.click(closeButton);

    expect(localStorage.getItem(customKey)).toBe('true');
  });
});
