import { createTheme, useMediaQuery } from '@mui/material';
import { breakpoints } from '@theme/breakpoints';
import { components } from '@theme/components';
import { darkPalette, palette as lightPalette } from '@theme/palette';
import { typography } from '@theme/typography';

/**
 * Custom React hook to retrieve the current MUI theme.
 * Merges system prefers-color-scheme preference with the custom/default palette,
 * breakpoints, typography, and components styles.
 *
 * @returns The instantiated MUI Theme object.
 */
export function useAppTheme() {
  const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)');
  const activePalette = prefersDarkMode ? darkPalette : lightPalette;

  return createTheme({
    breakpoints,
    palette: {
      ...activePalette,
      mode: prefersDarkMode ? 'dark' : 'light',
    },
    typography,
    components,
  });
}
export type UseAppThemeType = typeof useAppTheme;
