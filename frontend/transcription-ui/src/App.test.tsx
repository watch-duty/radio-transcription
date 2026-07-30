// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { ANNOUNCEMENT_CONFIG, type AnnouncementConfig } from './App';

describe('App Announcement Config Template', () => {
  it('defaults ANNOUNCEMENT_CONFIG to null when no survey or banner is active', () => {
    expect(ANNOUNCEMENT_CONFIG).toBeNull();
  });

  it('conforms to AnnouncementConfig type structure when populated', () => {
    const sampleConfig: AnnouncementConfig = {
      startDate: '2026-08-01T00:00:00',
      endDate: '2026-08-15T23:59:59',
      title: 'CSAT Survey:',
      message: 'Your feedback will help us improve this transcription tool.',
      linkUrl: 'https://forms.gle/test',
      linkText: 'Take survey',
    };

    expect(sampleConfig.startDate).toBe('2026-08-01T00:00:00');
    expect(sampleConfig.title).toBe('CSAT Survey:');
    expect(sampleConfig.linkUrl).toBe('https://forms.gle/test');
  });
});
