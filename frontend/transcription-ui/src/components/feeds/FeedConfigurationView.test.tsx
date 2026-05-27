// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';
import { VirtuosoMockContext } from 'react-virtuoso';

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
        <VirtuosoMockContext.Provider
          value={{ viewportHeight: 1000, itemHeight: 100 }}
        >
          <FeedConfigurationView
            triggerSnackbar={mockTriggerSnackbar}
            onError={mockOnError}
          />
        </VirtuosoMockContext.Provider>
      </MemoryRouter>
    );
  };

  it('renders both the creation form and the existing feeds list', async () => {
    renderView();

    // Verify Title
    expect(screen.getByText('Feed Configuration')).toBeInTheDocument();

    // Verify Creation form is present
    expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    const configCard = screen.getByTestId('feed-config-card');
    expect(
      within(configCard).getByLabelText('Display Name')
    ).toBeInTheDocument();
    expect(
      within(configCard).getByLabelText('Source Type')
    ).toBeInTheDocument();
    expect(
      within(configCard).getByLabelText('Source Feed ID')
    ).toBeInTheDocument();

    // Verify existing feeds list renders active items and their tag chips
    await waitFor(() => {
      expect(screen.getByText('Feeds')).toBeInTheDocument();
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });
    screen.debug(screen.getByTestId('feeds-deck-card'), 100000);
  });

  it('validates empty required fields and displays interactive errors', async () => {
    renderView();

    const submitBtn = screen.getByRole('button', {
      name: /Register feed/i,
    });
    fireEvent.click(submitBtn);

    // Verify validation errors are populated on screen
    expect(screen.getByText('Display name is required.')).toBeInTheDocument();
    expect(screen.getByText('Source feed ID is required.')).toBeInTheDocument();

    expect(createFeed).not.toHaveBeenCalled();
  });

  it('supports interactive key-value tags generation, adding, and deletion', async () => {
    renderView();

    const tagKeyInput = screen.getByLabelText('Key');
    const tagValInput = screen.getByLabelText('Value');
    const addTagBtn = screen.getByRole('button', { name: 'Add Tag' });

    // Try adding empty tag (should show error)
    fireEvent.click(addTagBtn);
    expect(
      screen.getByText('Both key and value must be populated to add a tag.')
    ).toBeInTheDocument();

    // Fill tag fields
    fireEvent.change(tagKeyInput, { target: { value: 'agency' } });
    fireEvent.change(tagValInput, { target: { value: 'CHP' } });
    fireEvent.click(addTagBtn);

    // Verify tag row is registered and populated as input values
    expect(screen.getAllByLabelText('Key')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Value')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Key')[1]).toHaveValue('agency');
    expect(screen.getAllByLabelText('Value')[1]).toHaveValue('CHP');

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

    // Delete tag using native delete button visual trigger
    const deleteButton = screen.getByRole('button', {
      name: 'Remove tag agency',
    });
    fireEvent.click(deleteButton);

    // Verify tag inputs row is successfully removed
    expect(
      screen.queryByRole('button', { name: 'Remove tag agency' })
    ).not.toBeInTheDocument();
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
    fireEvent.change(screen.getByLabelText('Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    // Select Source Type dropdown
    const selectDropdown = within(
      screen.getByTestId('feed-config-card')
    ).getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);

    // Wait for popover listbox portal to mount inside body
    const listbox = await screen.findByRole('listbox', {}, { timeout: 3000 });
    expect(listbox).toBeInTheDocument();

    const bcfyCallsOption =
      await within(listbox).findByText('Broadcastify Calls');
    fireEvent.click(bcfyCallsOption);

    fireEvent.change(screen.getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    // Submit
    const submitBtn = screen.getByRole('button', {
      name: /Register feed/i,
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
          externalId: '9988-77',
          tags: undefined,
        },
        'fake-jwt-token-xyz'
      );

      // Verify visual success alerts
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed "Napa Ambulance Dispatch" registered successfully!'
      );

      // Verify state clears on success
      expect(screen.getByLabelText('Display Name')).toHaveValue('');
      expect(screen.getByLabelText('Source Feed ID')).toHaveValue('');
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

    // Verify form transitions to "Edit Mode"
    expect(
      screen.getByText('Edit Feed: Marin Fire Dispatch')
    ).toBeInTheDocument();

    // Verify the row is highlighted
    const rowElement = screen
      .getByText('Marin Fire Dispatch')
      .closest('[role="row"]');
    expect(rowElement).toHaveClass('Mui-selected');

    // Verify form details are prepopulated from selected feed definition
    expect(screen.getByLabelText('Display Name')).toHaveValue(
      'Marin Fire Dispatch'
    );
    expect(screen.getByLabelText('Source Feed ID')).toHaveValue('33156');

    // Verify registered tag row is populated in input fields
    expect(screen.getAllByLabelText('Key')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Value')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Key')[1]).toHaveValue('county');
    expect(screen.getAllByLabelText('Value')[1]).toHaveValue('Marin');

    // Verify permanent fields are disabled in update mode
    const configCard = screen.getByTestId('feed-config-card');
    expect(within(configCard).getByLabelText('Source Type')).toHaveAttribute(
      'aria-disabled',
      'true'
    );
    expect(within(configCard).getByLabelText('Source Feed ID')).toBeDisabled();

    // Perform cancel edit check
    const cancelBtn = screen.getByRole('button', { name: /Cancel Edit/i });
    fireEvent.click(cancelBtn);

    // Verify transitions smoothly back to Create Mode
    expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    expect(screen.getByLabelText('Display Name')).toHaveValue('');
    expect(screen.getByLabelText('Source Feed ID')).not.toBeDisabled();

    // Verify the row loses selection highlight
    expect(rowElement).not.toHaveClass('Mui-selected');

    // Enter Edit Mode again to test saving
    fireEvent.click(editBtn);
    expect(
      screen.getByText('Edit Feed: Marin Fire Dispatch')
    ).toBeInTheDocument();

    // Update modifyable properties
    fireEvent.change(screen.getByLabelText('Display Name'), {
      target: { value: 'Marin Unified Fire Dispatch' },
    });

    // Submit update changes
    const submitBtn = screen.getByRole('button', {
      name: /Save changes/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      // Verify PUT service called
      expect(updateFeed).toHaveBeenCalledTimes(1);
      expect(updateFeed).toHaveBeenCalledWith(
        'feed-1',
        {
          name: 'Marin Unified Fire Dispatch',
          externalId: '33156',
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
      expect(screen.getByLabelText('Display Name')).toHaveValue('');
    });
  });

  it('filters active pipelines on the right side card container using feedSearchQuery filter bar', async () => {
    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });

    const filterInput = screen.getByPlaceholderText(/Search feeds/i);
    fireEvent.change(filterInput, { target: { value: 'sonoma' } });

    // Sonoma Sheriff matches, Marin Fire is hidden
    expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    expect(screen.queryByText('Marin Fire Dispatch')).not.toBeInTheDocument();
  });

  it('does not automatically add the tag if Tag Key and Tag Value are filled in but the Add button is not clicked', async () => {
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
    fireEvent.change(screen.getByLabelText('Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    // Select Source Type dropdown
    const selectDropdown = within(
      screen.getByTestId('feed-config-card')
    ).getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);
    const listbox = await screen.findByRole('listbox');
    const bcfyCallsOption =
      await within(listbox).findByText('Broadcastify Calls');
    fireEvent.click(bcfyCallsOption);

    fireEvent.change(screen.getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    // Fill in the tag inputs, but DO NOT click the plus button!
    fireEvent.change(screen.getByLabelText('Key'), {
      target: { value: 'county' },
    });
    fireEvent.change(screen.getByLabelText('Value'), {
      target: { value: 'Napa' },
    });

    // Submit
    const submitBtn = screen.getByRole('button', {
      name: /Register feed/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      expect(createFeed).toHaveBeenCalledTimes(1);
      // Expect tags to be undefined because they were not explicitly added using the 'Add' button
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Napa Ambulance Dispatch',
          sourceType: 'bcfy_calls',
          sourceFeedId: '9988-77',
          externalId: '9988-77',
          tags: undefined,
        },
        'fake-jwt-token-xyz'
      );
    });
  });

  it('supports sorting columns (Name, Type, Status) in both ascending and descending orders', async () => {
    renderView();

    // Verify initial sort: Name ascending
    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });

    const getRowNames = () => {
      const bodyRows = screen
        .getAllByRole('row')
        .filter((row) => row.getAttribute('data-item-index') !== null);
      return bodyRows.map(
        (row) => row.firstElementChild?.querySelector('p')?.textContent
      );
    };

    // Initial state check (Name asc: Marin then Sonoma)
    expect(getRowNames()).toEqual([
      'Marin Fire Dispatch',
      'Sonoma Sheriff dispatch',
    ]);

    // Sort by Name descending (Click Name header again)
    const nameHeader = screen.getByRole('button', { name: /name/i });
    fireEvent.click(nameHeader);
    expect(getRowNames()).toEqual([
      'Sonoma Sheriff dispatch',
      'Marin Fire Dispatch',
    ]);

    // Sort by Type ascending (Click Type header)
    const typeHeader = screen.getByRole('button', { name: /type/i });
    fireEvent.click(typeHeader);
    expect(getRowNames()).toEqual([
      'Marin Fire Dispatch',
      'Sonoma Sheriff dispatch',
    ]);

    // Sort by Type descending (Click Type header again)
    fireEvent.click(typeHeader);
    expect(getRowNames()).toEqual([
      'Sonoma Sheriff dispatch',
      'Marin Fire Dispatch',
    ]);

    // Sort by Status ascending (Click Status header)
    const statusHeader = screen.getByRole('button', { name: /status/i });
    fireEvent.click(statusHeader);
    expect(getRowNames()).toEqual([
      'Marin Fire Dispatch',
      'Sonoma Sheriff dispatch',
    ]);

    // Sort by Status descending (Click Status header again)
    fireEvent.click(statusHeader);
    expect(getRowNames()).toEqual([
      'Sonoma Sheriff dispatch',
      'Marin Fire Dispatch',
    ]);
  });
});
