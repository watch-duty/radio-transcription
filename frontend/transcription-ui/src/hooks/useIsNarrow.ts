import { useTheme } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';

// Shared narrow-viewport check (below the `sm` breakpoint). This covers any
// narrow window, not just mobile devices. `noSsr` evaluates the query before
// the first paint so the wider layout doesn't flash on mount.
export function useIsNarrow(): boolean {
  const theme = useTheme();
  return useMediaQuery(theme.breakpoints.down('sm'), { noSsr: true });
}
