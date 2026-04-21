import React, { useState } from 'react';

import { AuthContext } from './AuthContext';
import {
  clearAuthToken,
  getAuthToken,
  setAuthToken,
} from '../utils/authCookie';

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [token, setTokenState] = useState<string | null>(getAuthToken());

  const setToken = (newToken: string | null) => {
    if (newToken) {
      setAuthToken(newToken);
    } else {
      clearAuthToken();
    }
    setTokenState(newToken);
  };

  return (
    <AuthContext.Provider value={{ token, setToken }}>
      {children}
    </AuthContext.Provider>
  );
};
