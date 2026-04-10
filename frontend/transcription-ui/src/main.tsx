import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { GoogleOAuthProvider } from '@react-oauth/google';

import App from './App.tsx';
import { AuthProvider } from './context/AuthProvider';

import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <GoogleOAuthProvider clientId={import.meta.env.VITE_GOOGLE_AUTH_CLIENT_ID}>
      <AuthProvider>
        <App />
      </AuthProvider>
    </GoogleOAuthProvider>
  </StrictMode>
);
