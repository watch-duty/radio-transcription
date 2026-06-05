// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { ConfirmationDialog } from './ConfirmationDialog';

describe('ConfirmationDialog', () => {
  const mockClose = vi.fn();
  const mockConfirm = vi.fn();

  afterEach(() => {
    cleanup();
  });

  it('does not render when open is false', () => {
    render(
      <ConfirmationDialog
        open={false}
        title="Test Dialog"
        description="This is a test description"
        confirmLabel="Confirm"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    expect(screen.queryByText('Test Dialog')).not.toBeInTheDocument();
  });

  it('renders correctly with standard elements when open is true', () => {
    render(
      <ConfirmationDialog
        open={true}
        title="Test Dialog Title"
        description="Verify this action to proceed."
        confirmLabel="Go Ahead"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    expect(screen.getByText('Test Dialog Title')).toBeInTheDocument();
    expect(
      screen.getByText('Verify this action to proceed.')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: 'Go Ahead' })
    ).toBeInTheDocument();
  });

  it('calls onClose when Cancel button is clicked', () => {
    mockClose.mockClear();

    render(
      <ConfirmationDialog
        open={true}
        title="Test Dialog"
        description="Some message"
        confirmLabel="Confirm"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    fireEvent.click(cancelBtn);

    expect(mockClose).toHaveBeenCalledTimes(1);
  });

  it('calls onConfirm when Confirm/Action button is clicked', async () => {
    mockConfirm.mockClear();

    render(
      <ConfirmationDialog
        open={true}
        title="Test Dialog"
        description="Some message"
        confirmLabel="Submit Action"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    const confirmBtn = screen.getByRole('button', { name: 'Submit Action' });
    fireEvent.click(confirmBtn);

    expect(mockConfirm).toHaveBeenCalledTimes(1);
  });

  it('requires challenge matching if showConfirmInput is true', async () => {
    mockConfirm.mockClear();

    render(
      <ConfirmationDialog
        open={true}
        title="Challenge Mode"
        description="Requires typing ID"
        confirmLabel="Submit Deletion"
        showConfirmInput={true}
        confirmInputValue="MATCH-123"
        confirmInputLabel="Type MATCH-123 below:"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    const confirmBtn = screen.getByRole('button', { name: 'Submit Deletion' });
    expect(confirmBtn).toBeDisabled();

    const confirmInput = screen.getByLabelText('Type MATCH-123 below:');

    // Type incorrect value
    fireEvent.change(confirmInput, { target: { value: 'WRONG' } });
    expect(confirmBtn).toBeDisabled();

    // Type correct value
    fireEvent.change(confirmInput, { target: { value: 'MATCH-123' } });
    expect(confirmBtn).toBeEnabled();

    fireEvent.click(confirmBtn);
    expect(mockConfirm).toHaveBeenCalledTimes(1);
  });

  it('disables forms and action buttons when isSubmitting is true', () => {
    render(
      <ConfirmationDialog
        open={true}
        title="Submitting Mode"
        description="Wait for API call"
        confirmLabel="Proceed"
        showConfirmInput={true}
        confirmInputValue="RESET"
        confirmInputLabel="Type RESET below:"
        onClose={mockClose}
        onConfirm={mockConfirm}
        isSubmitting={true}
      />
    );

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    const confirmBtn = screen.getByRole('button', { name: 'Proceed' });
    const confirmInput = screen.getByLabelText('Type RESET below:');

    expect(cancelBtn).toBeDisabled();
    expect(confirmBtn).toBeDisabled();
    expect(confirmInput).toBeDisabled();
  });

  it('resets the typed challenge input when Cancel is clicked', async () => {
    mockClose.mockClear();

    render(
      <ConfirmationDialog
        open={true}
        title="Challenge Mode"
        description="Type matching ID"
        confirmLabel="Confirm"
        showConfirmInput={true}
        confirmInputValue="RESET"
        confirmInputLabel="Type RESET below:"
        onClose={mockClose}
        onConfirm={mockConfirm}
      />
    );

    const confirmInput = screen.getByLabelText('Type RESET below:');
    fireEvent.change(confirmInput, { target: { value: 'SOME-TYPED-DATA' } });
    expect(confirmInput).toHaveValue('SOME-TYPED-DATA');

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    fireEvent.click(cancelBtn);

    expect(mockClose).toHaveBeenCalledTimes(1);
    expect(confirmInput).toHaveValue('');
  });
});
