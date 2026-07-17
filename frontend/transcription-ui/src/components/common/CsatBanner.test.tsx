// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { CsatBanner } from './CsatBanner';

const TEST_START_DATE = new Date('2026-07-20T00:00:00');
const TEST_END_DATE = new Date('2026-07-29T23:59:59');
const TEST_FORM_URL = 'https://forms.gle/KocdXk8qWXyw7UCw9';

afterEach(() => {
  cleanup();
});

describe('CsatBanner', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('does not render before the start date', () => {
    const beforeStart = new Date(TEST_START_DATE.getTime() - 1000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={beforeStart}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('does not render after the end date', () => {
    const afterEnd = new Date(TEST_END_DATE.getTime() + 1000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={afterEnd}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('renders within the valid date window with default message and derived link text', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={duringWindow}
      />
    );
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();
    const link = screen.getByRole('link', {
      name: /https:\/\/forms\.gle\/KocdXk8qWXyw7UCw9 \(2 min survey\)/i,
    });
    expect(link.getAttribute('href')).toBe(TEST_FORM_URL);
  });

  it('supports string date formats and custom title/message/linkText props', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <CsatBanner
        startDate="2026-07-20T00:00:00"
        endDate="2026-07-29T23:59:59"
        formUrl="https://example.com/survey"
        title="Feedback:"
        message="Help us improve this tool!"
        linkText="Click here"
        currentDate={duringWindow}
      />
    );
    expect(screen.getByText('Feedback:')).toBeTruthy();
    expect(screen.getByText(/Help us improve this tool!/i)).toBeTruthy();
    const link = screen.getByRole('link', { name: 'Click here' });
    expect(link.getAttribute('href')).toBe('https://example.com/survey');
  });

  it('renders when forceShow is set to true even outside date window', () => {
    const beforeStart = new Date(TEST_START_DATE.getTime() - 1000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={beforeStart}
        forceShow
      />
    );
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();
  });

  it('closes and saves dismissal to localStorage when close button is clicked', () => {
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={duringWindow}
      />
    );
    expect(screen.getByText(/Please share your experience/i)).toBeTruthy();

    const closeButton = screen.getByRole('button', { name: /close/i });
    fireEvent.click(closeButton);

    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
    expect(
      localStorage.getItem(`survey_banner_dismissed_${TEST_FORM_URL}`)
    ).toBe('true');
  });

  it('does not render if previously dismissed in localStorage', () => {
    localStorage.setItem(`survey_banner_dismissed_${TEST_FORM_URL}`, 'true');
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        currentDate={duringWindow}
      />
    );
    expect(screen.queryByText(/Please share your experience/i)).toBeNull();
  });

  it('uses custom storageKey when provided', () => {
    const customKey = 'my_custom_survey_dismissal';
    const duringWindow = new Date(TEST_START_DATE.getTime() + 3600000);
    render(
      <CsatBanner
        startDate={TEST_START_DATE}
        endDate={TEST_END_DATE}
        formUrl={TEST_FORM_URL}
        storageKey={customKey}
        currentDate={duringWindow}
      />
    );

    const closeButton = screen.getByRole('button', { name: /close/i });
    fireEvent.click(closeButton);

    expect(localStorage.getItem(customKey)).toBe('true');
  });
});
