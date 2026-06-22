import type { ThemeOptions } from '@mui/material/styles';

export const palette: ThemeOptions['palette'] = {
  mode: 'light',
  primary: {
    main: '#1976d2',
  },
  secondary: {
    main: '#1976d2',
  },
  warning: {
    light: '#FDF0E8',
    main: '#EC7826',
    dark: '#EF8A43',
  },
  text: {
    primary: 'rgba(0, 0, 0, 0.87)',
    secondary: 'rgba(0, 0, 0, 0.6)',
  },
};

export const darkPalette: ThemeOptions['palette'] = {
  mode: 'dark',
  primary: {
    main: '#1976d2',
  },
  secondary: {
    main: '#1976d2',
  },
  warning: {
    light: '#FDF0E8',
    main: '#EC7826',
    dark: '#EF8A43',
  },
  text: {
    primary: '#fff',
    secondary: 'rgba(255, 255, 255, 0.7)',
  },
};
