import React, { useEffect, useState } from 'react';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';

import { authSession } from '../service/authSession';
import { AuthContext } from './AuthContext';

const REFRESH_TOKEN_INTERVAL = 50 * 60 * 1000; // 50 minutes
const REFRESH_TOKEN_FAILURE_DELAY = 30 * 1000; // 30 seconds

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  /**
   * Effect which checks the user's session.
   * Only runs once when the component mounts.
   */
  useEffect(() => {
    const checkSession = async () => {
      try {
        const token = await authSession();
        setToken(token);
      } catch (error) {
        console.error('Session check failed:', error);
      } finally {
        setLoading(false);
      }
    };

    checkSession();
  }, []);

  /**
   * Effect which performs a silent refresh of the user's session.
   * Runs 10 minutes before the token expires.
   */
  useEffect(() => {
    if (!token) return;

    const refreshTimer = setTimeout(async () => {
      let refreshed = false;
      while (!refreshed) {
        try {
          const token = await authSession();
          setToken(token);
          refreshed = true;
        } catch (error) {
          console.error('Refresh token failed:', error);
          // Wait before trying again.
          await new Promise((resolve) =>
            setTimeout(resolve, REFRESH_TOKEN_FAILURE_DELAY)
          );
        }
      }
    }, REFRESH_TOKEN_INTERVAL);

    return () => clearTimeout(refreshTimer);
  }, [token]);

  if (loading) {
    return (
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          width: '100vw',
          gap: 2,
        }}
      >
        <CircularProgress color="primary" />
        <Typography variant="body1">Loading...</Typography>
      </Box>
    );
  }

  return (
    <AuthContext.Provider value={{ token, setToken }}>
      {children}
    </AuthContext.Provider>
  );
};
