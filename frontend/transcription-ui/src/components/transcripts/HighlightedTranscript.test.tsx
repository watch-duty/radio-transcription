// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';
import type { RuleAnnotation } from '@transcription/common';

import HighlightedTranscript from './HighlightedTranscript';

function annotations(
  ...entries: Array<[string, Array<[number, number, string]>]>
): Record<string, RuleAnnotation> {
  return Object.fromEntries(
    entries.map(([ruleId, spans]) => [
      ruleId,
      {
        textMatch: spans.map(([startIndex, endIndex, matchedText]) => ({
          startIndex,
          endIndex,
          matchedText,
        })),
      },
    ])
  );
}

describe('HighlightedTranscript', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders plain text when there are no annotations', () => {
    render(
      <HighlightedTranscript
        text="all quiet on the western front"
        ruleAnnotations={{}}
      />
    );
    expect(screen.getByText('all quiet on the western front')).toBeTruthy();
  });

  it('renders plain text when annotations have no textMatch payload', () => {
    render(
      <HighlightedTranscript
        text="fire on the ridge"
        ruleAnnotations={{ r1: {} }}
      />
    );
    expect(screen.getByText('fire on the ridge')).toBeTruthy();
  });

  it('wraps matched spans in a styled span and preserves the original text', () => {
    const { container } = render(
      <HighlightedTranscript
        text="Fire on the ridge"
        ruleAnnotations={annotations(['r1', [[0, 4, 'Fire']]])}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(1);
    expect(highlights[0].textContent).toBe('Fire');
    expect(container.textContent).toBe('Fire on the ridge');
  });

  it('combines spans across multiple annotations in document order', () => {
    const { container } = render(
      <HighlightedTranscript
        text="evacuate the area, fire approaching"
        ruleAnnotations={annotations(
          ['r1', [[19, 23, 'fire']]],
          ['r2', [[0, 8, 'evacuate']]]
        )}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(2);
    expect(highlights[0].textContent).toBe('evacuate');
    expect(highlights[1].textContent).toBe('fire');
    expect(container.textContent).toBe('evacuate the area, fire approaching');
  });

  it('coalesces partially overlapping spans into a single highlight', () => {
    const { container } = render(
      <HighlightedTranscript
        text="firefighter"
        ruleAnnotations={annotations(
          ['r1', [[0, 5, 'firef']]],
          ['r2', [[3, 11, 'efighter']]]
        )}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(1);
    expect(highlights[0].textContent).toBe('firefighter');
    expect(container.textContent).toBe('firefighter');
  });

  it('ignores zero-width spans without warning', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { container } = render(
      <HighlightedTranscript
        text="fire on the ridge"
        ruleAnnotations={annotations(['r1', [[4, 4, '']]])}
      />
    );
    expect(container.querySelectorAll('span')).toHaveLength(0);
    expect(container.textContent).toBe('fire on the ridge');
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it('warns and drops inverted spans (endIndex before startIndex)', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { container } = render(
      <HighlightedTranscript
        text="fire on the ridge"
        ruleAnnotations={annotations(['r1', [[4, 1, 'erif']]])}
      />
    );
    expect(container.querySelectorAll('span')).toHaveLength(0);
    expect(container.textContent).toBe('fire on the ridge');
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0][0]).toContain('r1');
    warn.mockRestore();
  });

  it('drops shorter overlapping spans in favor of longer ones', () => {
    const { container } = render(
      <HighlightedTranscript
        text="a firefighter arrived"
        ruleAnnotations={annotations([
          'r1',
          [
            [2, 6, 'fire'],
            [2, 13, 'firefighter'],
          ],
        ])}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(1);
    expect(highlights[0].textContent).toBe('firefighter');
  });
});
