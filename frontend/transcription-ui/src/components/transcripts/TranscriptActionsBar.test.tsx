// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { TranscriptActionsBar } from './TranscriptActionsBar';

describe('TranscriptActionsBar', () => {
  const mockSetRedactTranscripts = vi.fn();

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

  it('renders the redact switch and toggles state when changed', () => {
    render(
      <TranscriptActionsBar
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
      />
    );

    const redactSwitch = screen.getByRole('switch', {
      name: /Redact transcripts/i,
    });
    expect(redactSwitch).toBeTruthy();
    expect((redactSwitch as HTMLInputElement).checked).toBe(false);

    fireEvent.click(redactSwitch);
    expect(mockSetRedactTranscripts).toHaveBeenCalledWith(true);
  });
});
