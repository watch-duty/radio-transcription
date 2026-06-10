import { describe, expect, it } from 'vitest';

import { hasEvaluationAlert } from './evaluationUtils';

describe('hasEvaluationAlert', () => {
  it('is true when there is at least one decision', () => {
    expect(hasEvaluationAlert(['rule-1'])).toBe(true);
  });

  it('is false for an empty list', () => {
    expect(hasEvaluationAlert([])).toBe(false);
  });

  it('is false when undefined', () => {
    expect(hasEvaluationAlert(undefined)).toBe(false);
  });
});
