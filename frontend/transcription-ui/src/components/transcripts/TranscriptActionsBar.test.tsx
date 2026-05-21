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
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        onClickViewLatest={vi.fn()}
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

  it('renders "Jump to latest" button when hasNewerTranscripts is true and calls onClickViewLatest when clicked', () => {
    const onClickViewLatest = vi.fn();
    render(
      <TranscriptActionsBar
        hasNewerTranscripts={true}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        onClickViewLatest={onClickViewLatest}
      />
    );

    const button = screen.getByRole('button', { name: /Jump to latest/i });
    expect(button).toBeTruthy();
    expect(button).not.toBeDisabled();

    fireEvent.click(button);
    expect(onClickViewLatest).toHaveBeenCalledTimes(1);
  });

  it('renders disabled "Viewing latest" button when hasNewerTranscripts is false', () => {
    render(
      <TranscriptActionsBar
        hasNewerTranscripts={false}
        redactTranscripts={false}
        setRedactTranscripts={mockSetRedactTranscripts}
        onClickViewLatest={vi.fn()}
      />
    );

    const button = screen.getByRole('button', { name: /Viewing latest/i });
    expect(button).toBeTruthy();
    expect(button).toBeDisabled();
  });
});
