// @vitest-environment jsdom
import React from 'react';
import { MemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { SourceType } from '@transcription/common';

import { AuthContext } from '../../context/AuthContext';
import * as listFeedsModule from '../../service/listFeeds';
import { SettingsView } from './SettingsView';

function createWrapper(token: string | null = 'mock-token') {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <AuthContext.Provider
        value={{
          token,
          setToken: vi.fn(),
          isAdmin: true,
          setIsAdmin: vi.fn(),
          isLoading: false,
          isError: false,
        }}
      >
        <QueryClientProvider client={queryClient}>
          <MemoryRouter>{children}</MemoryRouter>
        </QueryClientProvider>
      </AuthContext.Provider>
    );
  };
}

describe('SettingsView', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.spyOn(listFeedsModule, 'listFeeds').mockResolvedValue({
      feeds: [
        {
          id: 'feed-alpha',
          name: 'Alpha Feed',
          sourceType: SourceType.BCFY_FEEDS,
          status: 'active',
          substatus: 'active',
        },
        {
          id: 'feed-beta',
          name: 'Beta Feed',
          sourceType: SourceType.BCFY_FEEDS,
          status: 'active',
          substatus: 'active',
        },
      ],
      total: 2,
    });
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.restoreAllMocks();
  });

  it('renders SettingsView header and main section titles', () => {
    render(<SettingsView />, { wrapper: createWrapper() });

    expect(
      screen.getByRole('heading', { name: 'Settings' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { name: 'Theme Mode' })
    ).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Audio' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Feeds' })).toBeInTheDocument();
  });

  it('handles theme mode selection (System, Light, Dark) and dispatches custom events', () => {
    const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
    const triggerSnackbar = vi.fn();

    render(<SettingsView triggerSnackbar={triggerSnackbar} />, {
      wrapper: createWrapper(),
    });

    // Select Light
    const lightRadio = screen.getByLabelText('Light');
    fireEvent.click(lightRadio);

    expect(localStorage.getItem('radio.themeMode')).toBe('light');
    expect(dispatchSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'radio-theme-changed',
        detail: 'light',
      })
    );
    expect(triggerSnackbar).toHaveBeenCalledWith('Theme mode set to light');

    // Select Dark
    const darkRadio = screen.getByLabelText('Dark');
    fireEvent.click(darkRadio);

    expect(localStorage.getItem('radio.themeMode')).toBe('dark');
    expect(dispatchSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'radio-theme-changed',
        detail: 'dark',
      })
    );
    expect(triggerSnackbar).toHaveBeenCalledWith('Theme mode set to dark');

    // Select System Default
    const systemRadio = screen.getByLabelText('System Default');
    fireEvent.click(systemRadio);

    expect(localStorage.getItem('radio.themeMode')).toBe('system');
    expect(triggerSnackbar).toHaveBeenCalledWith('Theme mode set to system');
  });

  it('handles global default volume slider changes and reset', () => {
    const triggerSnackbar = vi.fn();
    render(<SettingsView triggerSnackbar={triggerSnackbar} />, {
      wrapper: createWrapper(),
    });

    const volumeSliderInput = screen.getByRole('slider', {
      name: 'Default Volume',
    });
    fireEvent.change(volumeSliderInput, { target: { value: '8' } });

    expect(localStorage.getItem('radio.audio.volumeDb')).toBe('8');

    // Click Volume Reset (first reset button)
    const resetButtons = screen.getAllByRole('button', { name: 'Reset' });
    fireEvent.click(resetButtons[0]);

    expect(localStorage.getItem('radio.audio.volumeDb')).toBeNull();
    expect(triggerSnackbar).toHaveBeenCalledWith(
      'Default volume reset to 0 dB'
    );
  });

  it('handles global default speed select changes and reset', () => {
    const triggerSnackbar = vi.fn();
    render(<SettingsView triggerSnackbar={triggerSnackbar} />, {
      wrapper: createWrapper(),
    });

    const speedSelect = screen.getByRole('combobox', {
      name: 'Default Playback Speed',
    });
    fireEvent.mouseDown(speedSelect);
    const option15 = screen.getByRole('option', { name: '1.5×' });
    fireEvent.click(option15);

    expect(localStorage.getItem('radio.audio.speed')).toBe('1.5');

    // Click Speed Reset (second reset button)
    const resetButtons = screen.getAllByRole('button', { name: 'Reset' });
    fireEvent.click(resetButtons[1]);

    expect(localStorage.getItem('radio.audio.speed')).toBeNull();
    expect(triggerSnackbar).toHaveBeenCalledWith(
      'Default playback speed reset to 1×'
    );
  });

  it('displays empty state message when no per-feed audio overrides exist', () => {
    render(<SettingsView />, { wrapper: createWrapper() });

    expect(
      screen.getByText(
        'No custom per-feed audio settings stored in localStorage.'
      )
    ).toBeInTheDocument();
  });

  it('loads stored per-feed overrides and maps feed names correctly', async () => {
    localStorage.setItem('radio.audio.volumeDb.feed-alpha', '6');
    localStorage.setItem('radio.audio.pan.feed-alpha', '-1');
    localStorage.setItem('radio.audio.speed.feed-alpha', '1.25');

    render(<SettingsView />, { wrapper: createWrapper() });

    const feedLink = await screen.findByRole('link', { name: 'Alpha Feed' });
    expect(feedLink).toBeInTheDocument();
    expect(feedLink.getAttribute('href')).toBe(
      '/transcripts?feedId=feed-alpha'
    );
    expect(screen.getAllByText('+6 dB').length).toBeGreaterThan(0);
  });

  it('allows inline editing of per-feed volume, pan, and speed', async () => {
    localStorage.setItem('radio.audio.volumeDb.feed-alpha', '6');
    localStorage.setItem('radio.audio.pan.feed-alpha', '0');
    localStorage.setItem('radio.audio.speed.feed-alpha', '1');

    render(<SettingsView />, { wrapper: createWrapper() });

    await screen.findByRole('link', { name: 'Alpha Feed' });

    // Change volume
    const feedVolumeSlider = screen.getByRole('slider', {
      name: 'Volume for Alpha Feed',
    });
    fireEvent.change(feedVolumeSlider, { target: { value: '-4' } });
    expect(localStorage.getItem('radio.audio.volumeDb.feed-alpha')).toBe('-4');

    // Change pan
    const panSelect = screen.getByRole('combobox', {
      name: 'Pan for Alpha Feed',
    });
    fireEvent.mouseDown(panSelect);
    const rightOption = screen.getByRole('option', { name: 'Right' });
    fireEvent.click(rightOption);
    expect(localStorage.getItem('radio.audio.pan.feed-alpha')).toBe('1');

    // Change speed
    const speedSelect = screen.getByRole('combobox', {
      name: 'Speed for Alpha Feed',
    });
    fireEvent.mouseDown(speedSelect);
    const speedOption2 = screen.getByRole('option', { name: '2×' });
    fireEvent.click(speedOption2);
    expect(localStorage.getItem('radio.audio.speed.feed-alpha')).toBe('2');
  });

  it('allows adding a new feed override using Autocomplete', async () => {
    const triggerSnackbar = vi.fn();
    render(<SettingsView triggerSnackbar={triggerSnackbar} />, {
      wrapper: createWrapper(),
    });

    const autocompleteInput = await screen.findByLabelText(
      'Select feed to override audio settings'
    );
    fireEvent.mouseDown(autocompleteInput);

    const optionBeta = await screen.findByRole('option', {
      name: 'Beta Feed (feed-beta)',
    });
    fireEvent.click(optionBeta);

    const addButton = screen.getByRole('button', { name: 'Add override' });
    fireEvent.click(addButton);

    expect(localStorage.getItem('radio.audio.volumeDb.feed-beta')).toBe('0');
    expect(localStorage.getItem('radio.audio.pan.feed-beta')).toBe('0');
    expect(localStorage.getItem('radio.audio.speed.feed-beta')).toBe('1');
    expect(triggerSnackbar).toHaveBeenCalledWith(
      'Added custom audio override for Beta Feed'
    );
  });

  it('allows removing a per-feed override', async () => {
    localStorage.setItem('radio.audio.volumeDb.feed-alpha', '3');
    localStorage.setItem('radio.audio.pan.feed-alpha', '1');
    localStorage.setItem('radio.audio.speed.feed-alpha', '2');

    const triggerSnackbar = vi.fn();
    render(<SettingsView triggerSnackbar={triggerSnackbar} />, {
      wrapper: createWrapper(),
    });

    await screen.findByRole('link', { name: 'Alpha Feed' });

    const deleteBtn = screen.getByLabelText('Remove override for Alpha Feed');
    fireEvent.click(deleteBtn);

    expect(localStorage.getItem('radio.audio.volumeDb.feed-alpha')).toBeNull();
    expect(localStorage.getItem('radio.audio.pan.feed-alpha')).toBeNull();
    expect(localStorage.getItem('radio.audio.speed.feed-alpha')).toBeNull();
    expect(triggerSnackbar).toHaveBeenCalledWith(
      'Removed custom audio settings for Alpha Feed'
    );
  });

  it('calls onError callback when feed list query fails', async () => {
    const apiError = new Error('Failed to fetch feeds');
    vi.spyOn(listFeedsModule, 'listFeeds').mockRejectedValue(apiError);

    const onError = vi.fn();
    render(<SettingsView onError={onError} />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(onError).toHaveBeenCalledWith(
        apiError,
        'Loading Feeds for Settings'
      );
    });
  });
});
