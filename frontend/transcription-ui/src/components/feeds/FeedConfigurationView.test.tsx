// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import type { Feed } from '@transcription/common';

import { createFeed } from '../../service/createFeed';
import { listFeeds } from '../../service/listFeeds';
import { updateFeed } from '../../service/updateFeed';
import { renderWithQueryClient } from '../../test/testUtils';
import FeedConfigurationView from './FeedConfigurationView';

// Mock API services
vi.mock('../../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

vi.mock('../../service/createFeed', () => ({
  createFeed: vi.fn(),
}));

vi.mock('../../service/updateFeed', () => ({
  updateFeed: vi.fn(),
}));

// Mock AuthContext
vi.mock('../../context/AuthContext', () => ({
  useAuth: () => ({ token: 'fake-jwt-token-xyz' }),
}));

describe('FeedConfigurationView', () => {
  const mockTriggerSnackbar = vi.fn();
  const mockOnError = vi.fn();

  const mockFeeds: Feed[] = [
    {
      id: 'feed-1',
      name: 'Marin Fire Dispatch',
      sourceType: 'bcfy_feeds',
      sourceFeedId: '33156',
      externalId: 'ca-mrn-fire',
      status: 'active',
      tags: [{ key: 'county', value: 'Marin' }],
    },
    {
      id: 'feed-2',
      name: 'Sonoma Sheriff dispatch',
      sourceType: 'openmhz',
      sourceFeedId: 'sonoma-county',
      externalId: 'ca-snm-sheriff',
      status: 'inactive',
      tags: [{ key: 'county', value: 'Sonoma' }],
    },
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    mockTriggerSnackbar.mockClear();
    mockOnError.mockClear();

    // Default mock for listing feeds
    vi.mocked(listFeeds).mockResolvedValue(mockFeeds);

    // Mock window.scrollTo since JSDOM does not implement it
    window.scrollTo = vi.fn();
  });

  afterEach(() => {
    cleanup();
  });

  const renderView = () => {
    return renderWithQueryClient(
      <MemoryRouter>
        <FeedConfigurationView
          triggerSnackbar={mockTriggerSnackbar}
          onError={mockOnError}
        />
      </MemoryRouter>
    );
  };

  it('renders both the creation form and the existing feeds list', async () => {
    renderView();

    // Verify Title
    expect(screen.getByText('Feed Configuration')).toBeInTheDocument();

    // Verify Creation form is present
    expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    expect(screen.getByLabelText('Feed Display Name')).toBeInTheDocument();
    expect(screen.getByLabelText('Source Type')).toBeInTheDocument();
    expect(screen.getByLabelText('Source Feed ID')).toBeInTheDocument();
    expect(screen.getByLabelText('External Indexing ID')).toBeInTheDocument();

    // Verify existing feeds list renders active items
    await waitFor(() => {
      expect(screen.getByText('Registered feeds')).toBeInTheDocument();
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });
  });

  it('validates empty required fields and displays interactive errors', async () => {
    renderView();

    const submitBtn = screen.getByRole('button', {
      name: /Register Pipeline Feed/i,
    });
    fireEvent.click(submitBtn);

    // Verify validation errors are populated on screen
    expect(
      screen.getByText('Feed Display Name is required.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Source Feed ID is required to register a feed.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('External Reference ID is required.')
    ).toBeInTheDocument();

    expect(createFeed).not.toHaveBeenCalled();
  });

  it('updates the Source Feed ID helper text dynamically based on Source Type dropdown selection', async () => {
    renderView();

    // Initial default: Broadcastify Audio Feed (bcfy_feeds)
    expect(
      screen.getByText(/Broadcastify audio feed number/i)
    ).toBeInTheDocument();

    // Click to change dropdown to OpenMHZ
    const selectDropdown = screen.getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);

    // MUI dropdown opens in popover, locate OpenMHZ menu item
    const openMhzOption = await screen.findByRole('option', {
      name: /OpenMHZ Trunked System/i,
    });
    fireEvent.click(openMhzOption);

    // Verify ID description helper text changes dynamically
    expect(screen.getByText(/OpenMHZ system slug name/i)).toBeInTheDocument();
  });

  it('supports interactive key-value tags generation, adding, and deletion', async () => {
    renderView();

    const tagKeyInput = screen.getByLabelText('Tag Key');
    const tagValInput = screen.getByLabelText('Tag Value');
    const addTagBtn = screen.getByRole('button', { name: 'Add Tag' });

    // Try adding empty tag (should show error)
    fireEvent.click(addTagBtn);
    expect(
      screen.getByText(
        'Both Tag Key and Value must be populated to register a tag.'
      )
    ).toBeInTheDocument();

    // Fill tag fields
    fireEvent.change(tagKeyInput, { target: { value: 'agency' } });
    fireEvent.change(tagValInput, { target: { value: 'CHP' } });
    fireEvent.click(addTagBtn);

    // Verify tag chip is registered and displayed
    const formCard = screen.getByTestId('feed-config-card');
    expect(within(formCard).getByText('agency')).toBeInTheDocument();
    expect(within(formCard).getByText(/: CHP/)).toBeInTheDocument();

    // Clear tag fields on success
    expect(tagKeyInput).toHaveValue('');
    expect(tagValInput).toHaveValue('');

    // Try adding duplicate key (should show warning)
    fireEvent.change(tagKeyInput, { target: { value: 'agency' } });
    fireEvent.change(tagValInput, { target: { value: 'Sheriff' } });
    fireEvent.click(addTagBtn);
    expect(
      screen.getByText('A tag with key "agency" already exists.')
    ).toBeInTheDocument();

    // Delete tag using chip delete trigger
    const deleteButton = within(formCard).getByTestId('CancelIcon');
    fireEvent.click(deleteButton);

    // Verify chip is successfully removed
    expect(screen.queryByText(/CHP/i)).not.toBeInTheDocument();
  });

  it('submits form successfully, registers feed in API, calls snackbar and clears state', async () => {
    const mockCreatedFeed = {
      id: 'feed-99',
      name: 'Napa Ambulance Dispatch',
      sourceType: 'bcfy_calls' as const,
      sourceFeedId: '9988-77',
      externalId: 'ca-nap-amb',
      status: 'active' as const,
    };
    vi.mocked(createFeed).mockResolvedValue(mockCreatedFeed);

    renderView();

    // Input form details
    fireEvent.change(screen.getByLabelText('Feed Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    // Select Source Type dropdown
    const selectDropdown = screen.getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);
    const bcfyCallsOption = await screen.findByRole('option', {
      name: /Broadcastify Talkgroup Calls/i,
    });
    fireEvent.click(bcfyCallsOption);

    fireEvent.change(screen.getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });
    fireEvent.change(screen.getByLabelText('External Indexing ID'), {
      target: { value: 'ca-nap-amb' },
    });

    // Submit
    const submitBtn = screen.getByRole('button', {
      name: /Register Pipeline Feed/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      // Verify API service is called with mapped values
      expect(createFeed).toHaveBeenCalledTimes(1);
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Napa Ambulance Dispatch',
          sourceType: 'bcfy_calls',
          sourceFeedId: '9988-77',
          externalId: 'ca-nap-amb',
          tags: undefined,
        },
        'fake-jwt-token-xyz'
      );

      // Verify visual success alerts
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed "Napa Ambulance Dispatch" registered successfully!'
      );

      // Verify state clears on success
      expect(screen.getByLabelText('Feed Display Name')).toHaveValue('');
      expect(screen.getByLabelText('Source Feed ID')).toHaveValue('');
      expect(screen.getByLabelText('External Indexing ID')).toHaveValue('');
    });
  });

  it('supports dynamic transition to Edit Mode, Locks permanent fields, cancels edit context and saves changes', async () => {
    const mockUpdatedFeed = {
      ...mockFeeds[0],
      name: 'Marin Unified Fire Dispatch',
      externalId: 'ca-mrn-fire-v2',
      tags: [
        { key: 'county', value: 'Marin' },
        { key: 'region', value: 'West' },
      ],
    };
    vi.mocked(updateFeed).mockResolvedValue(mockUpdatedFeed);

    renderView();

    // Verify existing feeds list
    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });

    // Locate edit click action icon next to Marin Fire Dispatch (first item)
    const editBtn = screen.getByRole('button', {
      name: 'Edit Marin Fire Dispatch',
    });
    fireEvent.click(editBtn);

    // Verify form transitions to "Update Mode"
    expect(screen.getByText('Update Feed')).toBeInTheDocument();
    expect(screen.getByText(/Modifying configuration/i)).toBeInTheDocument();
    expect(
      screen.getByText(/Live Update Protocol Active/i)
    ).toBeInTheDocument();

    // Verify form details are prepopulated from selected feed definition
    expect(screen.getByLabelText('Feed Display Name')).toHaveValue(
      'Marin Fire Dispatch'
    );
    expect(screen.getByLabelText('Source Feed ID')).toHaveValue('33156');
    expect(screen.getByLabelText('External Indexing ID')).toHaveValue(
      'ca-mrn-fire'
    );

    const formCard = screen.getByTestId('feed-config-card');
    expect(within(formCard).getByText('county')).toBeInTheDocument();
    expect(within(formCard).getByText(/: Marin/)).toBeInTheDocument();

    // Verify permanent fields are disabled in update mode
    expect(screen.getByLabelText('Source Type')).toHaveAttribute(
      'aria-disabled',
      'true'
    );
    expect(screen.getByLabelText('Source Feed ID')).toBeDisabled();

    // Perform cancel edit check
    const cancelBtn = screen.getByRole('button', { name: /Cancel Edit/i });
    fireEvent.click(cancelBtn);

    // Verify transitions smoothly back to Create Mode
    expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    expect(screen.getByLabelText('Feed Display Name')).toHaveValue('');
    expect(screen.getByLabelText('Source Feed ID')).not.toBeDisabled();

    // Enter Edit Mode again to test saving
    fireEvent.click(editBtn);
    expect(screen.getByText('Update Feed')).toBeInTheDocument();

    // Update modifyable properties
    fireEvent.change(screen.getByLabelText('Feed Display Name'), {
      target: { value: 'Marin Unified Fire Dispatch' },
    });
    fireEvent.change(screen.getByLabelText('External Indexing ID'), {
      target: { value: 'ca-mrn-fire-v2' },
    });

    // Submit update changes
    const submitBtn = screen.getByRole('button', {
      name: /Save Feed Changes/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      // Verify PUT service called
      expect(updateFeed).toHaveBeenCalledTimes(1);
      expect(updateFeed).toHaveBeenCalledWith(
        'feed-1',
        {
          name: 'Marin Unified Fire Dispatch',
          externalId: 'ca-mrn-fire-v2',
          tags: [{ key: 'county', value: 'Marin' }],
        },
        'fake-jwt-token-xyz'
      );

      // Verify success visuals
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed "Marin Unified Fire Dispatch" updated successfully!'
      );

      // Verify returns back to Create Mode
      expect(screen.getByText('Register New Feed')).toBeInTheDocument();
      expect(screen.getByLabelText('Feed Display Name')).toHaveValue('');
    });
  });

  it('filters active pipelines on the right side card container using feedSearchQuery filter bar', async () => {
    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });

    const filterInput = screen.getByPlaceholderText(/Filter receiver streams/i);
    fireEvent.change(filterInput, { target: { value: 'sonoma' } });

    // Sonoma Sheriff matches, Marin Fire is hidden
    expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    expect(screen.queryByText('Marin Fire Dispatch')).not.toBeInTheDocument();
  });
});
