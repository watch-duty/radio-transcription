import React, { useCallback, useEffect, useRef, useState } from 'react';

import { decodeJwt } from 'jose';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';

import { useUserInfo } from '../hooks/useUserInfo';
import { authSession } from '../service/authSession';
import { AuthContext } from './AuthContext';

const REFRESH_TOKEN_INTERVAL = 55 * 60 * 1000; // 55 minutes
const REFRESH_TOKEN_FAILURE_DELAY = 30 * 1000; // 30 seconds
const MAX_REFRESH_ATTEMPTS = 10; // Max attempts to refresh token

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [token, setToken] = useState<string | null>(null);
  const [isCheckingSession, setIsCheckingSession] = useState(true);
  const isRefreshingRef = useRef(false);

  const {
    data: userInfo,
    isLoading: isUserInfoLoading,
    isError: isUserInfoError,
  } = useUserInfo(token);

  const isAdmin = userInfo?.isAdmin ?? false;
  const setIsAdmin = useCallback(() => {
    if (import.meta.env.DEV) {
      console.warn(
        'setIsAdmin is deprecated: isAdmin is derived directly from backend user info.'
      );
    }
  }, []);

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
        setIsCheckingSession(false);
      }
    };

    checkSession();
  }, []);

  /**
   * Effect which performs a silent refresh of the user's session.
   * Runs 10 minutes before the token expires.
   * Also listens for visibility changes and window focus to refresh expired or soon-to-expire tokens.
   */
  useEffect(() => {
    if (!token) return;

    const checkAndRefresh = async () => {
      if (isRefreshingRef.current) return;
      try {
        const decoded = decodeJwt(token);
        const isExpiredOrSoon = decoded.exp
          ? decoded.exp * 1000 - Date.now() < 5 * 60 * 1000
          : true;

        if (isExpiredOrSoon) {
          isRefreshingRef.current = true;
          try {
            const newToken = await authSession();
            setToken(newToken);
          } catch (error) {
            setToken(null);
            throw error;
          }
        }
      } catch (error) {
        console.error(
          'Failed to check/refresh token on visibility change/focus:',
          error
        );
      } finally {
        isRefreshingRef.current = false;
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        checkAndRefresh();
      }
    };

    const handleFocus = () => {
      checkAndRefresh();
    };

    const refreshTimer = setTimeout(async () => {
      let refreshed = false;
      let attempts = 0;
      while (!refreshed && attempts < MAX_REFRESH_ATTEMPTS) {
        attempts++;
        try {
          const token = await authSession();
          setToken(token);
          refreshed = true;
        } catch (error) {
          console.error('Refresh token failed:', error);
          if (attempts >= MAX_REFRESH_ATTEMPTS) {
            setToken(null);
          } else {
            // Wait before trying again.
            await new Promise((resolve) =>
              setTimeout(resolve, REFRESH_TOKEN_FAILURE_DELAY)
            );
          }
        }
      }
    }, REFRESH_TOKEN_INTERVAL);

    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('focus', handleFocus);

    return () => {
      clearTimeout(refreshTimer);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      window.removeEventListener('focus', handleFocus);
    };
  }, [token]);

  if (isCheckingSession) {
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
    <AuthContext.Provider
      value={{
        token,
        setToken,
        isAdmin,
        setIsAdmin,
        isLoading: isUserInfoLoading,
        isError: isUserInfoError,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
