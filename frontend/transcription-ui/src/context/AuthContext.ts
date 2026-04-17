import { createContext, useContext } from 'react';

export const AuthContext = createContext<{
  token: string | null;
  setToken: (token: string | null) => void;
}>({ token: null, setToken: () => {} });

export const useAuth = () => useContext(AuthContext);
