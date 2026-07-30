// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { fireEvent, render, screen } from '@testing-library/react';
import type { FeedHistoryEvent } from '@transcription/common';

import { AuditRow } from './AuditRow';

describe('AuditRow', () => {
  const mockEvent: FeedHistoryEvent = {
    id: 'evt_123',
    feedId: 'feed_123',
    action: 'feed.recovered',
    actor: 'system',
    occurredAt: Date.parse('2026-06-26T12:34:56.000Z'),
    feedRevision: 2,
    beforeValues: { status: 'failing' },
    afterValues: { status: 'active' },
  };

  it('renders the audit action details correctly', () => {
    render(<AuditRow auditEvent={mockEvent} />);

    expect(screen.getByText(/Feed recovered successfully/)).toBeTruthy();
    expect(screen.getByText('FAILING')).toBeTruthy();
    expect(screen.getByText('ACTIVE')).toBeTruthy();
  });

  it('renders manual actions with administrator actor ID', () => {
    const manualEvent = {
      ...mockEvent,
      action: 'feed.reset',
      actor: 'user:google:admin@example.com',
    };
    render(<AuditRow auditEvent={manualEvent} />);
    expect(screen.getByText(/by user:google:admin@example.com/)).toBeTruthy();
  });

  it('renders diff changes for updated fields like name and tags', () => {
    const updateEvent: FeedHistoryEvent = {
      id: 'evt_update',
      feedId: 'feed_123',
      action: 'feed.updated',
      actor: 'user:admin@example.com',
      occurredAt: Date.parse('2026-06-26T13:00:00Z'),
      feedRevision: 3,
      beforeValues: {
        name: 'Old Name',
        tags: [{ key: 'county', value: 'Marin' }],
      },
      afterValues: {
        name: 'New Name',
        tags: [
          { key: 'county', value: 'Marin' },
          { key: 'agency', value: 'Fire' },
        ],
      },
    };

    render(<AuditRow auditEvent={updateEvent} />);

    expect(
      screen.getByText('• name changed from "Old Name" to "New Name"')
    ).toBeTruthy();
    expect(screen.getByText('• Tags: added "agency=Fire"')).toBeTruthy();
  });

  it('renders a popover with the failure reason and details when present', () => {
    const failureEvent: FeedHistoryEvent = {
      ...mockEvent,
      action: 'feed.failure_reported',
      beforeValues: { status: 'active' },
      afterValues: {
        status: 'failing',
        statusReason: 'Connection timed out',
        statusReasonDetail: 'Failed to connect to Icecast server on port 8000.',
      },
    };

    render(<AuditRow auditEvent={failureEvent} />);

    // Click the info icon button
    const infoButton = screen.getByRole('button');
    fireEvent.click(infoButton);

    // Verify the popover content appears
    expect(screen.getByText('Connection timed out')).toBeTruthy();
    expect(
      screen.getByText('Failed to connect to Icecast server on port 8000.')
    ).toBeTruthy();
  });
});
