// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { ScannerProvider } from '../../context/ScannerProvider';
import { AddToScannerButton } from './AddToScannerButton';

const feed = { id: 'feed-1', name: 'Test Feed' };

const renderButton = (variant: 'icon' | 'button' = 'icon') =>
  render(
    <ScannerProvider>
      <AddToScannerButton feed={feed} variant={variant} />
    </ScannerProvider>
  );

describe('AddToScannerButton', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    cleanup();
  });

  it('toggles a feed in and out of the scanner', () => {
    renderButton('icon');

    const button = screen.getByLabelText(/Add to Scanner: Test Feed/i);
    fireEvent.click(button);

    expect(
      screen.getByLabelText(/Remove from Scanner: Test Feed/i)
    ).toBeTruthy();

    fireEvent.click(screen.getByLabelText(/Remove from Scanner: Test Feed/i));

    expect(screen.getByLabelText(/Add to Scanner: Test Feed/i)).toBeTruthy();
  });

  it('reflects added state in the button variant label', () => {
    renderButton('button');

    expect(screen.getByText('Add to Scanner')).toBeTruthy();
    fireEvent.click(screen.getByRole('button'));
    expect(screen.getByText('In Scanner')).toBeTruthy();
  });
});
