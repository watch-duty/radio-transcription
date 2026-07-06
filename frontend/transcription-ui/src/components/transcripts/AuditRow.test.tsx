// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { render, screen } from '@testing-library/react';
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

    expect(screen.getByText('[System Event]')).toBeTruthy();
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
});
