import React, { useEffect } from 'react';
import { Navigate, useLocation } from 'react-router';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';

import { useAuth } from '../../context/AuthContext';
import { useUserInfo } from '../../hooks/useUserInfo';

export const RequireAdmin = ({ children }: { children: React.ReactNode }) => {
  const { token, setIsAdmin } = useAuth();
  const location = useLocation();
  const { data: userInfo, isLoading, isError } = useUserInfo(token);

  // We check useUserInfo locally to obtain the query's loading and error states.
  // Since AuthProvider renders the application before the user info query finishes,
  // we must wait for this query to resolve (showing a loading spinner in the meantime)
  // to prevent premature redirection to the home page while isAdmin is still false.
  // React Query will automatically deduplicate this request with the one in AuthProvider.
  useEffect(() => {
    if (userInfo) {
      setIsAdmin(userInfo.isAdmin);
    } else if (isError) {
      setIsAdmin(false);
    }
  }, [userInfo, isError, setIsAdmin]);

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

  if (isError || !userInfo?.isAdmin) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
