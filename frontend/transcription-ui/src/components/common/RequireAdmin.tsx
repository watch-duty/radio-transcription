import React, { useEffect, useState } from 'react';
import { Navigate, useLocation } from 'react-router';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';

import { useAuth } from '../../context/AuthContext';
import { getUserInfo } from '../../service/getUserInfo';

export const RequireAdmin = ({ children }: { children: React.ReactNode }) => {
  const { token, isAdmin, setIsAdmin } = useAuth();
  const [checking, setChecking] = useState(!isAdmin);
  const location = useLocation();

  useEffect(() => {
    let active = true;
    const verifyAdmin = async () => {
      if (!token) {
        setChecking(false);
        return;
      }
      try {
        const info = await getUserInfo(token);
        if (active) {
          setIsAdmin(info.isAdmin);
        }
      } catch (error) {
        console.error('Failed to verify admin status on navigation:', error);
        if (active) {
          setIsAdmin(false);
        }
      } finally {
        if (active) {
          setChecking(false);
        }
      }
    };

    verifyAdmin();
    return () => {
      active = false;
    };
  }, [token, location.pathname, setIsAdmin]);

  if (!token) {
    return <Navigate to="/login" replace state={{ from: location }} />;
  }

  if (checking) {
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

  if (!isAdmin) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
