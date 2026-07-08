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
import type {
  BackendFeedStatus,
  Feed,
  FeedStatus,
} from '@transcription/common';
import { SourceType } from '@transcription/common';

import { createFeed } from '../../service/createFeed';
import { deactivateFeed } from '../../service/deactivateFeed';
import { deleteFeed } from '../../service/deleteFeed';
import { listFeeds } from '../../service/listFeeds';
import { resetFeed } from '../../service/resetFeed';
import { updateFeed } from '../../service/updateFeed';
import { renderWithQueryClient } from '../../test/testUtils';
import FeedConfigurationView from './FeedConfigurationView';

// Need to mock out the list because otherwise the list is too long and these tests will
// time out.
vi.hoisted(() => {
  if (typeof Intl !== 'undefined') {
    Intl.supportedValuesOf = (key: string) => {
      if (key === 'timeZone') {
        return ['America/Los_Angeles', 'America/New_York', 'UTC'];
      }
      return [];
    };
  }
});

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

vi.mock('../../service/deleteFeed', () => ({
  deleteFeed: vi.fn(),
}));

vi.mock('../../service/deactivateFeed', () => ({
  deactivateFeed: vi.fn(),
}));

vi.mock('../../service/resetFeed', () => ({
  resetFeed: vi.fn(),
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
      sourceType: SourceType.BCFY_FEEDS,
      sourceFeedId: '33156',
      status: 'active',
      substatus: 'active',
      tags: [{ key: 'county', value: 'Marin' }],
    },
    {
      id: 'feed-2',
      name: 'Sonoma Sheriff dispatch',
      sourceType: SourceType.OPENMHZ,
      sourceFeedId: 'sonoma-county',
      status: 'inactive',
      substatus: 'deactivated',
      tags: [{ key: 'county', value: 'Sonoma' }],
    },
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    mockTriggerSnackbar.mockClear();
    mockOnError.mockClear();

    // Default mock for listing feeds
    vi.mocked(listFeeds).mockImplementation((_, params) => {
      let filtered = mockFeeds;
      if (params?.name) {
        const q = params.name.toLowerCase();
        filtered = filtered.filter((f) => f.name.toLowerCase().includes(q));
      }
      return Promise.resolve({ feeds: filtered, total: filtered.length });
    });
    vi.mocked(deleteFeed).mockResolvedValue(undefined);
    vi.mocked(deactivateFeed).mockResolvedValue(undefined);
    vi.mocked(resetFeed).mockResolvedValue({} as Feed);

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
    const formCard = screen.getByTestId('feed-config-card');
    expect(within(formCard).getByLabelText('Display Name')).toBeInTheDocument();
    expect(within(formCard).getByLabelText('Source Type')).toBeInTheDocument();
    expect(
      within(formCard).getByLabelText('Source Feed ID')
    ).toBeInTheDocument();

    // Verify existing feeds list renders active items and their tag chips
    await waitFor(() => {
      expect(screen.getByText('Feeds')).toBeInTheDocument();
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });
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

    // Try adding empty tag (should NOT show error because nothing was added/entered yet)
    fireEvent.click(addTagBtn);
    expect(
      screen.queryByText('Both key and value must be populated to add a tag.')
    ).not.toBeInTheDocument();

    // Try adding partial tag (should show error since they typed a key but no value)
    fireEvent.change(tagKeyInput, { target: { value: 'agency' } });
    fireEvent.click(addTagBtn);
    expect(
      screen.getByText('Both key and value must be populated to add a tag.')
    ).toBeInTheDocument();

    // Change value (should automatically clear tag validation error)
    fireEvent.change(tagValInput, { target: { value: 'CHP' } });
    expect(
      screen.queryByText('Both key and value must be populated to add a tag.')
    ).not.toBeInTheDocument();

    // Click add (should save changes successfully since inputs are now complete)
    fireEvent.click(addTagBtn);

    // Verify tag row is registered and populated as input values
    expect(screen.getAllByLabelText('Key')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Value')[0]).toHaveValue('');
    expect(screen.getAllByLabelText('Key')[1]).toHaveValue('agency');
    expect(screen.getAllByLabelText('Value')[1]).toHaveValue('CHP');

    // Clear tag fields on success
    expect(tagKeyInput).toHaveValue('');
    expect(tagValInput).toHaveValue('');

    // Adding a duplicate key now succeeds for multi-value keys (e.g. a feed
    // covering multiple counties/agencies).
    fireEvent.change(tagKeyInput, { target: { value: 'agency' } });
    fireEvent.change(tagValInput, { target: { value: 'Sheriff' } });
    fireEvent.click(addTagBtn);
    expect(
      screen.queryByText('A tag with key "agency" already exists.')
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /agency=CHP/ })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /agency=Sheriff/ })
    ).toBeInTheDocument();

    // Deleting one row removes only that row, not every row sharing the key.
    fireEvent.click(screen.getByRole('button', { name: /agency=CHP/ }));
    expect(
      screen.queryByRole('button', { name: /agency=CHP/ })
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /agency=Sheriff/ })
    ).toBeInTheDocument();
  });

  it('submits form successfully, registers feed in API, calls snackbar and clears state', async () => {
    const mockCreatedFeed = {
      id: 'feed-99',
      name: 'Napa Ambulance Dispatch',
      sourceType: SourceType.BCFY_CALLS,
      sourceFeedId: '9988-77',
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
    };
    vi.mocked(createFeed).mockResolvedValue(mockCreatedFeed);

    renderView();

    const formCard = screen.getByTestId('feed-config-card');

    // Input form details
    fireEvent.change(within(formCard).getByLabelText('Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    // Select Source Type dropdown
    const selectDropdown = within(formCard).getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);

    // Wait for popover listbox portal to mount inside body
    const listbox = await screen.findByRole('listbox', {}, { timeout: 3000 });
    expect(listbox).toBeInTheDocument();

    const bcfyCallsOption =
      await within(listbox).findByText('Broadcastify Calls');
    fireEvent.click(bcfyCallsOption);

    fireEvent.change(within(formCard).getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    // Submit
    const submitBtn = within(formCard).getByRole('button', {
      name: /Register feed/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      // Verify API service is called with mapped values
      expect(createFeed).toHaveBeenCalledTimes(1);
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Napa Ambulance Dispatch',
          sourceType: SourceType.BCFY_CALLS,
          sourceFeedId: '9988-77',
          tags: [],
        },
        'fake-jwt-token-xyz'
      );

      // Verify visual success alerts
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed "Napa Ambulance Dispatch" registered successfully!'
      );

      // Verify state clears on success
      expect(within(formCard).getByLabelText('Display Name')).toHaveValue('');
      expect(within(formCard).getByLabelText('Source Feed ID')).toHaveValue('');
    });
  });

  it('supports dynamic transition to Edit Mode, Locks permanent fields, cancels edit context and saves changes', async () => {
    const mockUpdatedFeed = {
      ...mockFeeds[0],
      name: 'Marin Unified Fire Dispatch',
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

    // Retrieve fresh Edit Form Card after mode transition
    const editFormCard = screen.getByTestId('feed-config-card');

    // Verify form details are prepopulated from selected feed definition
    expect(within(editFormCard).getByLabelText('Display Name')).toHaveValue(
      'Marin Fire Dispatch'
    );
    expect(within(editFormCard).getByLabelText('Source Feed ID')).toHaveValue(
      '33156'
    );

    // Verify registered tag row is populated in input fields
    expect(within(editFormCard).getAllByLabelText('Key')[0]).toHaveValue('');
    expect(within(editFormCard).getAllByLabelText('Value')[0]).toHaveValue('');
    expect(within(editFormCard).getAllByLabelText('Key')[1]).toHaveValue(
      'county'
    );
    expect(within(editFormCard).getAllByLabelText('Value')[1]).toHaveValue(
      'Marin'
    );

    // Verify permanent fields are disabled in update mode
    expect(within(editFormCard).getByLabelText('Source Type')).toHaveAttribute(
      'aria-disabled',
      'true'
    );
    expect(
      within(editFormCard).getByLabelText('Source Feed ID')
    ).toBeDisabled();

    // Perform cancel edit check
    const cancelBtn = within(editFormCard).getByRole('button', {
      name: /Cancel Edit/i,
    });
    fireEvent.click(cancelBtn);

    // Verify transitions smoothly back to Create Mode
    expect(screen.getByText('Register New Feed')).toBeInTheDocument();

    // Retrieve fresh Register Form Card after unmount/mount transition
    const registerFormCard = screen.getByTestId('feed-config-card');
    expect(within(registerFormCard).getByLabelText('Display Name')).toHaveValue(
      ''
    );
    expect(
      within(registerFormCard).getByLabelText('Source Feed ID')
    ).not.toBeDisabled();

    // Verify the row loses selection highlight
    expect(rowElement).not.toHaveClass('Mui-selected');

    // Enter Edit Mode again to test saving
    fireEvent.click(editBtn);
    expect(
      screen.getByText('Edit Feed: Marin Fire Dispatch')
    ).toBeInTheDocument();

    // Retrieve fresh Edit Form Card
    const editFormCard2 = screen.getByTestId('feed-config-card');

    // Update modifyable properties
    fireEvent.change(within(editFormCard2).getByLabelText('Display Name'), {
      target: { value: 'Marin Unified Fire Dispatch' },
    });

    // Submit update changes
    const submitBtn = within(editFormCard2).getByRole('button', {
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

      // Retrieve fresh Register Form Card
      const finalFormCard = screen.getByTestId('feed-config-card');
      expect(within(finalFormCard).getByLabelText('Display Name')).toHaveValue(
        ''
      );
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
    await waitFor(() => {
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
      expect(screen.queryByText('Marin Fire Dispatch')).not.toBeInTheDocument();
    });
  });

  it('automatically adds the tag if Tag Key and Tag Value are filled in and the form is submitted without clicking the Add button', async () => {
    const mockCreatedFeed = {
      id: 'feed-99',
      name: 'Napa Ambulance Dispatch',
      sourceType: SourceType.BCFY_CALLS,
      sourceFeedId: '9988-77',
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
    };
    vi.mocked(createFeed).mockResolvedValue(mockCreatedFeed);

    renderView();

    const formCard = screen.getByTestId('feed-config-card');

    // Input form details
    fireEvent.change(within(formCard).getByLabelText('Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    // Select Source Type dropdown
    const selectDropdown = within(formCard).getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);
    const listbox = await screen.findByRole('listbox');
    const bcfyCallsOption =
      await within(listbox).findByText('Broadcastify Calls');
    fireEvent.click(bcfyCallsOption);

    fireEvent.change(within(formCard).getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    // Fill in the tag inputs, but DO NOT click the plus button!
    fireEvent.change(within(formCard).getByLabelText('Key'), {
      target: { value: 'county' },
    });
    fireEvent.change(within(formCard).getByLabelText('Value'), {
      target: { value: 'Napa' },
    });

    // Submit
    const submitBtn = within(formCard).getByRole('button', {
      name: /Register feed/i,
    });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      expect(createFeed).toHaveBeenCalledTimes(1);
      // Expect tags to be automatically included since they were entered in the input fields
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Napa Ambulance Dispatch',
          sourceType: SourceType.BCFY_CALLS,
          sourceFeedId: '9988-77',
          tags: [{ key: 'county', value: 'Napa' }],
        },
        'fake-jwt-token-xyz'
      );

      // Expect tag fields to be cleared on success along with other fields due to form reset
      expect(within(formCard).getByLabelText('Key')).toHaveValue('');
      expect(within(formCard).getByLabelText('Value')).toHaveValue('');
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
        (row) => row.firstElementChild?.querySelector('a')?.textContent
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

  it('displays validation error if partial tag inputs are provided without clicking Add upon form submission', async () => {
    renderView();

    const formCard = screen.getByTestId('feed-config-card');

    // Input form details
    fireEvent.change(within(formCard).getByLabelText('Display Name'), {
      target: { value: 'Napa Ambulance Dispatch' },
    });

    fireEvent.change(within(formCard).getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    // Fill only the key input, leave value empty!
    fireEvent.change(within(formCard).getByLabelText('Key'), {
      target: { value: 'county' },
    });

    // Submit
    const submitBtn = within(formCard).getByRole('button', {
      name: /Register feed/i,
    });
    fireEvent.click(submitBtn);

    // Expect validation error to show and prevent API call
    expect(
      screen.getByText('Both key and value must be populated to add a tag.')
    ).toBeInTheDocument();
    expect(createFeed).not.toHaveBeenCalled();
  });

  it('should show Delete feed button under edit mode, open confirmation dialog, and call deleteFeed API on confirm', async () => {
    vi.mocked(deleteFeed).mockResolvedValue(undefined);

    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });

    const editBtn = screen.getByRole('button', {
      name: 'Edit Marin Fire Dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');

    // Feed actions kebab button is present
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    expect(kebabBtn).toBeInTheDocument();

    // Click kebab button
    fireEvent.click(kebabBtn);

    // Kebab menu option "Delete feed" is present
    const deleteMenuItem = screen.getByRole('menuitem', {
      name: /Delete feed/i,
    });
    expect(deleteMenuItem).toBeInTheDocument();

    // Click Delete feed menu option
    fireEvent.click(deleteMenuItem);

    // Verify confirmation dialog shows
    expect(screen.getByText('Verify Feed Deletion')).toBeInTheDocument();
    expect(
      screen.getByText(
        'Are you sure you want to delete the feed? This will remove the feed and any associated metadata (e.g. transcripts, annotations, etc.). This action is not reversible.'
      )
    ).toBeInTheDocument();

    const confirmDeleteBtn = screen.getByRole('button', {
      name: 'Delete',
    });
    expect(confirmDeleteBtn).toBeDisabled();

    const confirmInput = screen.getByLabelText(
      'To confirm, type the Source Feed ID "33156" below:'
    );

    // Type incorrect ID
    fireEvent.change(confirmInput, { target: { value: 'wrong-id-xyz' } });
    expect(confirmDeleteBtn).toBeDisabled();

    // Type correct ID
    fireEvent.change(confirmInput, { target: { value: '33156' } });
    expect(confirmDeleteBtn).toBeEnabled();

    // Click confirm/delete action
    fireEvent.click(confirmDeleteBtn);

    await waitFor(() => {
      expect(deleteFeed).toHaveBeenCalledTimes(1);
      expect(deleteFeed).toHaveBeenCalledWith('feed-1', 'fake-jwt-token-xyz');

      // Verify success visuals
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed deleted successfully!'
      );

      // Verify transitions back to Create Mode
      expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    });
  });

  it('should not call deleteFeed API if delete confirmation is cancelled', async () => {
    vi.mocked(deleteFeed).mockResolvedValue(undefined);

    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });

    const editBtn = screen.getByRole('button', {
      name: 'Edit Marin Fire Dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    expect(kebabBtn).toBeInTheDocument();

    fireEvent.click(kebabBtn);

    const deleteMenuItem = screen.getByRole('menuitem', {
      name: /Delete feed/i,
    });
    expect(deleteMenuItem).toBeInTheDocument();

    fireEvent.click(deleteMenuItem);

    expect(screen.getByText('Verify Feed Deletion')).toBeInTheDocument();

    const cancelDeleteBtn = screen.getByRole('button', {
      name: 'Cancel',
    });
    fireEvent.click(cancelDeleteBtn);

    // Dialog should be gone
    await waitFor(() => {
      expect(
        screen.queryByText('Verify Feed Deletion')
      ).not.toBeInTheDocument();
    });

    expect(deleteFeed).not.toHaveBeenCalled();
  });

  it('should show Deactivate feed menu item if feed is not deactivated, and call deactivateFeed API on confirm', async () => {
    vi.mocked(deactivateFeed).mockResolvedValue(undefined);

    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });

    // Click Edit to enter edit mode (substatus is active, which is not deactivated)
    const editBtn = screen.getByRole('button', {
      name: 'Edit Marin Fire Dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    expect(kebabBtn).toBeInTheDocument();

    // Open kebab menu
    fireEvent.click(kebabBtn);

    // Menu option "Deactivate feed" should be visible Since substatus is NOT deactivated
    const deactivateMenuItem = screen.getByRole('menuitem', {
      name: /Deactivate feed/i,
    });
    expect(deactivateMenuItem).toBeInTheDocument();

    // Click Deactivate
    fireEvent.click(deactivateMenuItem);

    // Verify confirmation dialog shows
    expect(screen.getByText('Verify Feed Deactivation')).toBeInTheDocument();

    const confirmDeactivateBtn = screen.getByRole('button', {
      name: 'Deactivate',
    });
    fireEvent.click(confirmDeactivateBtn);

    await waitFor(() => {
      expect(deactivateFeed).toHaveBeenCalledTimes(1);
      expect(deactivateFeed).toHaveBeenCalledWith(
        'feed-1',
        'fake-jwt-token-xyz'
      );
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed deactivated successfully!'
      );
      expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    });
  });

  it('should not show Deactivate feed menu item if feed is deactivated', async () => {
    renderView();

    await waitFor(() => {
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });

    // Sonoma Sheriff dispatch has substatus: deactivated
    const editBtn = screen.getByRole('button', {
      name: 'Edit Sonoma Sheriff dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    fireEvent.click(kebabBtn);

    // "Deactivate feed" should not be rendered if deactivated
    expect(
      screen.queryByRole('menuitem', { name: /Deactivate feed/i })
    ).not.toBeInTheDocument();
  });

  it('should show Reset feed menu item if feed status is not active, and call resetFeed API on confirm', async () => {
    const mockResetResult: Feed = {
      id: 'feed-2',
      name: 'Sonoma Sheriff dispatch',
      sourceType: SourceType.OPENMHZ,
      sourceFeedId: 'sonoma-county',
      status: 'inactive',
      substatus: 'unclaimed',
      tags: [{ key: 'county', value: 'Sonoma' }],
    };
    vi.mocked(resetFeed).mockResolvedValue(mockResetResult);

    renderView();

    await waitFor(() => {
      expect(screen.getByText('Sonoma Sheriff dispatch')).toBeInTheDocument();
    });

    // Click Edit for Sonoma Sheriff dispatch (status inactive)
    const editBtn = screen.getByRole('button', {
      name: 'Edit Sonoma Sheriff dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    fireEvent.click(kebabBtn);

    // "Reset feed" should show since status is inactive (not active)
    const resetMenuItem = screen.getByRole('menuitem', {
      name: /Reset feed/i,
    });
    expect(resetMenuItem).toBeInTheDocument();

    fireEvent.click(resetMenuItem);

    expect(screen.getByText('Verify Feed Reset')).toBeInTheDocument();

    const confirmResetBtn = screen.getByRole('button', {
      name: 'Reset',
    });
    fireEvent.click(confirmResetBtn);

    await waitFor(() => {
      expect(resetFeed).toHaveBeenCalledTimes(1);
      expect(resetFeed).toHaveBeenCalledWith('feed-2', 'fake-jwt-token-xyz');
      expect(mockTriggerSnackbar).toHaveBeenCalledWith(
        'Feed "Sonoma Sheriff dispatch" reset successfully!'
      );
      expect(screen.getByText('Register New Feed')).toBeInTheDocument();
    });
  });

  it('should not show Reset feed menu item if feed status is active', async () => {
    renderView();

    await waitFor(() => {
      expect(screen.getByText('Marin Fire Dispatch')).toBeInTheDocument();
    });

    // Marin Fire Dispatch status is active
    const editBtn = screen.getByRole('button', {
      name: 'Edit Marin Fire Dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    fireEvent.click(kebabBtn);

    // "Reset feed" must not show since status is active
    expect(
      screen.queryByRole('menuitem', { name: /Reset feed/i })
    ).not.toBeInTheDocument();
  });

  it('should not show Reset feed menu item if feed substatus is unclaimed', async () => {
    const unclaimedFeed: Feed = {
      id: 'feed-3',
      name: 'Oakland Fire Dispatch',
      sourceType: SourceType.BCFY_FEEDS,
      sourceFeedId: '44556',
      status: 'inactive',
      substatus: 'unclaimed',
      tags: [],
    };
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: [...mockFeeds, unclaimedFeed],
      total: mockFeeds.length + 1,
    });

    renderView();

    await waitFor(() => {
      expect(screen.getByText('Oakland Fire Dispatch')).toBeInTheDocument();
    });

    const editBtn = screen.getByRole('button', {
      name: 'Edit Oakland Fire Dispatch',
    });
    fireEvent.click(editBtn);

    const editFormCard = screen.getByTestId('feed-config-card');
    const kebabBtn = within(editFormCard).getByRole('button', {
      name: /feed actions/i,
    });
    fireEvent.click(kebabBtn);

    expect(
      screen.queryByRole('menuitem', { name: /Reset feed/i })
    ).not.toBeInTheDocument();
  });

  it('handles timezone tag selection using dropdown', async () => {
    renderView();

    const tagKeyInput = screen.getByLabelText('Key');
    const addTagBtn = screen.getByRole('button', { name: 'Add Tag' });

    // Change key to 'system/timezone'
    fireEvent.change(tagKeyInput, { target: { value: 'system/timezone' } });

    // The 'Value' input should be a select since the tag is a timezone.
    const timezoneSelect = screen.getByRole('combobox', { name: /Timezone/i });
    expect(timezoneSelect).toBeInTheDocument();

    // Click to open the dropdown
    fireEvent.mouseDown(timezoneSelect);

    const options = screen.getAllByRole('option');
    fireEvent.click(options[0]);

    // Check that the tag is added with a dropdown for editing
    fireEvent.click(addTagBtn);
    expect(screen.getAllByLabelText('Key')[1]).toHaveValue('system/timezone');
    const addedTimezoneSelect = screen.getAllByRole('combobox', {
      name: /Timezone/i,
    })[0];
    expect(addedTimezoneSelect).toHaveTextContent('UTC');
  });

  it('rejects a tag beyond its numberOfValues limit (system/timezone allows 1)', async () => {
    renderView();

    const addTagBtn = screen.getByRole('button', { name: 'Add Tag' });

    // Add a first system/timezone tag.
    fireEvent.change(screen.getByLabelText('Key'), {
      target: { value: 'system/timezone' },
    });
    fireEvent.mouseDown(screen.getByRole('combobox', { name: /Timezone/i }));
    fireEvent.click(screen.getAllByRole('option')[0]);
    fireEvent.click(addTagBtn);

    // Attempt a second one: the empty new-tag key input is the first Key field.
    fireEvent.change(screen.getAllByLabelText('Key')[0], {
      target: { value: 'system/timezone' },
    });
    fireEvent.mouseDown(
      screen.getAllByRole('combobox', { name: /Timezone/i })[0]
    );
    fireEvent.click(screen.getAllByRole('option')[0]);
    fireEvent.click(addTagBtn);

    expect(
      screen.getByText('The key "system/timezone" allows at most 1 value.')
    ).toBeInTheDocument();
  });

  it('rejects an exact key+value duplicate tag', async () => {
    renderView();

    const tagKeyInput = screen.getByLabelText('Key');
    const tagValInput = screen.getByLabelText('Value');
    const addTagBtn = screen.getByRole('button', { name: 'Add Tag' });

    const addRegion = (value: string) => {
      fireEvent.change(tagKeyInput, { target: { value: 'region' } });
      fireEvent.change(tagValInput, { target: { value } });
      fireEvent.click(addTagBtn);
    };

    addRegion('elbert-CO_US');
    // A different value for the same key is allowed (multi-county).
    addRegion('denver-CO_US');
    // An exact repeat is not.
    addRegion('elbert-CO_US');

    expect(
      screen.getByText('The tag "region=elbert-CO_US" already exists.')
    ).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /region=/i })).toHaveLength(2);
  });

  it('submits multiple values for the same key (multi-county feed)', async () => {
    const mockCreatedFeed = {
      id: 'feed-99',
      name: 'Multi-County Dispatch',
      sourceType: SourceType.BCFY_CALLS,
      sourceFeedId: '9988-77',
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
    };
    vi.mocked(createFeed).mockResolvedValue(mockCreatedFeed);

    renderView();

    const formCard = screen.getByTestId('feed-config-card');

    fireEvent.change(within(formCard).getByLabelText('Display Name'), {
      target: { value: 'Multi-County Dispatch' },
    });

    const selectDropdown = within(formCard).getByRole('combobox', {
      name: /Source Type/i,
    });
    fireEvent.mouseDown(selectDropdown);
    const listbox = await screen.findByRole('listbox');
    fireEvent.click(await within(listbox).findByText('Broadcastify Calls'));

    fireEvent.change(within(formCard).getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    const addTagBtn = within(formCard).getByRole('button', { name: 'Add Tag' });
    const addRegion = (value: string) => {
      fireEvent.change(within(formCard).getAllByLabelText('Key')[0], {
        target: { value: 'region' },
      });
      fireEvent.change(within(formCard).getAllByLabelText('Value')[0], {
        target: { value },
      });
      fireEvent.click(addTagBtn);
    };
    addRegion('sonoma-CA_US');
    addRegion('elbert-CO_US');

    fireEvent.click(
      within(formCard).getByRole('button', { name: /Register feed/i })
    );

    await waitFor(() => {
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Multi-County Dispatch',
          sourceType: SourceType.BCFY_CALLS,
          sourceFeedId: '9988-77',
          tags: [
            { key: 'region', value: 'sonoma-CA_US' },
            { key: 'region', value: 'elbert-CO_US' },
          ],
        },
        'fake-jwt-token-xyz'
      );
    });
  });

  it('allows saving when a row is edited into a duplicate (only add-time dupes are blocked)', async () => {
    vi.mocked(createFeed).mockResolvedValue({
      id: 'feed-99',
      name: 'Multi-County Dispatch',
      sourceType: SourceType.BCFY_CALLS,
      sourceFeedId: '9988-77',
      status: 'active' as FeedStatus,
      substatus: 'active' as BackendFeedStatus,
    });

    renderView();

    const formCard = screen.getByTestId('feed-config-card');

    fireEvent.change(within(formCard).getByLabelText('Display Name'), {
      target: { value: 'Multi-County Dispatch' },
    });
    fireEvent.mouseDown(
      within(formCard).getByRole('combobox', { name: /Source Type/i })
    );
    const listbox = await screen.findByRole('listbox');
    fireEvent.click(await within(listbox).findByText('Broadcastify Calls'));
    fireEvent.change(within(formCard).getByLabelText('Source Feed ID'), {
      target: { value: '9988-77' },
    });

    const addTagBtn = within(formCard).getByRole('button', { name: 'Add Tag' });
    const addRegion = (value: string) => {
      fireEvent.change(within(formCard).getAllByLabelText('Key')[0], {
        target: { value: 'region' },
      });
      fireEvent.change(within(formCard).getAllByLabelText('Value')[0], {
        target: { value },
      });
      fireEvent.click(addTagBtn);
    };
    addRegion('sonoma-CA_US');
    addRegion('elbert-CO_US');

    // Value inputs: [0] = empty new-tag input, [1] = first row, [2] = second row.
    // Editing the second row to collide with the first is not an add-time dup,
    // so it is allowed through.
    const valueInputs = within(formCard).getAllByLabelText('Value');
    fireEvent.change(valueInputs[2], { target: { value: 'sonoma-CA_US' } });

    fireEvent.click(
      within(formCard).getByRole('button', { name: /Register feed/i })
    );

    await waitFor(() => {
      expect(createFeed).toHaveBeenCalledWith(
        {
          name: 'Multi-County Dispatch',
          sourceType: SourceType.BCFY_CALLS,
          sourceFeedId: '9988-77',
          tags: [
            { key: 'region', value: 'sonoma-CA_US' },
            { key: 'region', value: 'sonoma-CA_US' },
          ],
        },
        'fake-jwt-token-xyz'
      );
    });
  });
});
