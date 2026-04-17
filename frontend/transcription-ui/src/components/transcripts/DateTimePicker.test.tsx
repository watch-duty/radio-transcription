// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';

import DateTimePicker from './DateTimePicker';

afterEach(() => {
  cleanup();
});

describe('DateTimePicker', () => {
  it('renders successfully with label', () => {
    render(
      <DateTimePicker
        label="Test Picker"
        dateTime={null}
        setDateTime={vi.fn()}
      />
    );
    expect(screen.getAllByLabelText('Test Picker')[0]).toBeTruthy();
  });

  it('shows error highlight and helper text', () => {
    render(
      <DateTimePicker
        label="Test Picker"
        dateTime={null}
        setDateTime={vi.fn()}
        error={true}
        helperText="Must be before End Time"
      />
    );
    expect(screen.getByText('Must be before End Time')).toBeTruthy();
  });
});
