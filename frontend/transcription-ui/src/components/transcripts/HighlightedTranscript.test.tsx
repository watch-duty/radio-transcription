// @vitest-environment jsdom
import { afterEach, describe, expect, it } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';
import type { RuleAnnotation } from '@transcription/common';

import HighlightedTranscript from './HighlightedTranscript';

function textMatch(
  ruleId: string,
  spans: Array<[number, number, string]>
): RuleAnnotation {
  return {
    ruleId,
    textMatch: {
      spans: spans.map(([start, end, matchedText]) => ({
        start,
        end,
        matchedText,
      })),
    },
  };
}

describe('HighlightedTranscript', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders plain text when there are no annotations', () => {
    render(
      <HighlightedTranscript
        text="all quiet on the western front"
        ruleAnnotations={[]}
      />
    );
    expect(screen.getByText('all quiet on the western front')).toBeTruthy();
  });

  it('renders plain text when annotations have no textMatch payload', () => {
    render(
      <HighlightedTranscript
        text="fire on the ridge"
        ruleAnnotations={[{ ruleId: 'r1' }]}
      />
    );
    expect(screen.getByText('fire on the ridge')).toBeTruthy();
  });

  it('wraps matched spans in a styled span and preserves the original text', () => {
    const { container } = render(
      <HighlightedTranscript
        text="Fire on the ridge"
        ruleAnnotations={[textMatch('r1', [[0, 4, 'Fire']])]}
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
        ruleAnnotations={[
          textMatch('r1', [[19, 23, 'fire']]),
          textMatch('r2', [[0, 8, 'evacuate']]),
        ]}
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
        ruleAnnotations={[
          textMatch('r1', [[0, 5, 'firef']]),
          textMatch('r2', [[3, 11, 'efighter']]),
        ]}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(1);
    expect(highlights[0].textContent).toBe('firefighter');
    expect(container.textContent).toBe('firefighter');
  });

  it('drops shorter overlapping spans in favor of longer ones', () => {
    const { container } = render(
      <HighlightedTranscript
        text="a firefighter arrived"
        ruleAnnotations={[
          textMatch('r1', [
            [2, 6, 'fire'],
            [2, 13, 'firefighter'],
          ]),
        ]}
      />
    );
    const highlights = container.querySelectorAll('span');
    expect(highlights).toHaveLength(1);
    expect(highlights[0].textContent).toBe('firefighter');
  });
});
