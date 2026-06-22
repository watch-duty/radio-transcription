import type { ThemeOptions } from '@mui/material/styles';

export const components: ThemeOptions['components'] = {
  MuiBadge: {
    styleOverrides: {
      badge: ({ ownerState, theme }) => ({
        ...(ownerState.color === 'default' && {
          backgroundColor: theme.palette.text.secondary,
          color: theme.palette.background.paper,
        }),
      }),
    },
  },
  MuiLink: {
    styleOverrides: {
      root: ({ theme }) => ({
        color: theme.palette.primary.contrastText,
      }),
    },
  },
};
