import { Link as RouterLink } from 'react-router';

import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';

export interface NavigationBreadcrumbsProps {
  routes: { name: string; url: string }[];
}

export function NavigationBreadcrumbs({ routes }: NavigationBreadcrumbsProps) {
  return (
    <Breadcrumbs
      aria-label="breadcrumb"
      separator={<NavigateNextIcon fontSize="small" />}
    >
      {routes.map((route, ii) => {
        if (ii < routes.length - 1) {
          return (
            <Link
              component={RouterLink}
              to={route.url}
              underline="hover"
              sx={{ display: 'flex', alignItems: 'center' }}
            >
              {route.name}
            </Link>
          );
        } else {
          return (
            <Typography key={ii} sx={{ color: 'text.primary' }}>
              {route.name}
            </Typography>
          );
        }
      })}
    </Breadcrumbs>
  );
}
