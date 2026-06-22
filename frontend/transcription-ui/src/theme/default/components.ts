import type { ThemeOptions } from '@mui/material/styles';

export const components: ThemeOptions['components'] = {
  MuiBadge: {
    styleOverrides: {
      badge: ({ ownerState, theme }) => ({
        ...(ownerState.color === 'default' && {
          backgroundColor: theme.vars.palette.text.secondary,
          color: theme.vars.palette.background.paper,
        }),
        ...(ownerState.color === 'primary' && {
          backgroundColor: theme.vars.palette.secondary.main,
          color: theme.vars.palette.primary.contrastText,
          fontWeight: 'bold',
        }),
      }),
    },
  },
  MuiLink: {
    styleOverrides: {
      root: ({ theme }) => ({
        color: theme.vars.palette.secondary.contrastText,
      }),
    },
  },
  MuiAppBar: {
    defaultProps: {
      enableColorOnDark: true,
    },
    styleOverrides: {
      root: ({ theme }) => ({
        backgroundColor: theme.vars.palette.secondary.main,
      }),
      colorPrimary: ({ theme }) => ({
        backgroundColor: theme.vars.palette.secondary.main,
      }),
    },
  },
};
