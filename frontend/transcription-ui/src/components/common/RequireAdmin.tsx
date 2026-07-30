import React from 'react';
import { Navigate, useLocation } from 'react-router';

import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';

import { useAuth } from '../../context/AuthContext';

export const RequireAdmin = ({ children }: { children: React.ReactNode }) => {
  const { token, isAdmin, isLoading, isError } = useAuth();
  const location = useLocation();

  if (!token) {
    return <Navigate to="/login" replace state={{ from: location }} />;
  }

  if (isLoading) {
    return (
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          width: '100vw',
        }}
      >
        <CircularProgress color="primary" />
      </Box>
    );
  }

  if (isError) {
    return (
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          width: '100vw',
        }}
      >
        <Alert severity="error">
          Failed to load user permissions. Please try again.
        </Alert>
      </Box>
    );
  }

  if (!isAdmin) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
