import { extendTheme } from '@mui/material/styles';
import { breakpoints } from '@theme/breakpoints';
import { components } from '@theme/components';
import { darkPalette, palette as lightPalette } from '@theme/palette';
import { typography } from '@theme/typography';

const theme = extendTheme({
  cssVarPrefix: 'mui',
  colorSchemeSelector: 'media',
  breakpoints,
  typography,
  components,
  colorSchemes: {
    light: {
      palette: lightPalette,
    },
    dark: {
      palette: darkPalette,
    },
  },
});

export function useAppTheme() {
  return theme;
}
export type UseAppThemeType = typeof useAppTheme;
