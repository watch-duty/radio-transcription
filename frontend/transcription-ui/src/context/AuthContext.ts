import { createContext, useContext } from 'react';

interface AuthContextType {
  token: string | null;
  setToken: (token: string | null) => void;
  isAdmin: boolean;
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
