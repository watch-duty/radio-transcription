import { createContext, useContext } from 'react';

interface AuthContextType {
  token: string | null;
  setToken: (token: string | null) => void;
  isAdmin: boolean;
  /** @deprecated setIsAdmin is deprecated. isAdmin is derived directly from backend user info. */
  setIsAdmin: (isAdmin: boolean) => void;
  isLoading: boolean;
  isError: boolean;
}

export const AuthContext = createContext<AuthContextType>({
  token: null,
  setToken: () => {},
  isAdmin: false,
  setIsAdmin: () => {},
  isLoading: false,
  isError: false,
});

export const useAuth = () => useContext(AuthContext);
