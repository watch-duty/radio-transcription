import { useState } from 'react';
import { useNavigate } from 'react-router';

import Alert from '@mui/material/Alert';
import { useTheme } from '@mui/material/styles';
import { useGoogleLogin } from '@react-oauth/google';
import { AppProvider } from '@toolpad/core/AppProvider';
import { SignInPage } from '@toolpad/core/SignInPage';
import { ApiError } from '@transcription/common';

import { useAuth } from '../context/AuthContext';
import { authLogin } from '../service/authLogin';

export function Login() {
  const theme = useTheme();
  const navigate = useNavigate();
  const { setToken } = useAuth();

  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const login = useGoogleLogin({
    onSuccess: async ({ code }) => {
      try {
        const token = await authLogin(code);
        setToken(token);
        const isInternal =
          document.referrer && document.referrer.includes(window.location.host);
        if (isInternal) {
          navigate(-1);
        } else {
          navigate('/', { replace: true });
        }
      } catch (error: unknown) {
        if (error instanceof ApiError) {
          setErrorMessage('Unable to sign in: ' + error.message);
        } else {
          setErrorMessage('Unable to sign in. Please try again.');
        }
      }
    },
    onError: () => {
      setErrorMessage('Unable to sign in. Please try again.');
    },
    flow: 'auth-code',
  });

  function LoginAlert() {
    return (
      <Alert
        sx={{ mb: 2, px: 1, py: 0.25, width: '100%', textAlign: 'left' }}
        severity="error"
      >
        {errorMessage}
      </Alert>
    );
  }

  return (
    <AppProvider theme={theme}>
      <SignInPage
        providers={[{ id: 'google', name: 'Google' }]}
        signIn={async () => {
          if (import.meta.env.MODE === 'local-dev') {
            setErrorMessage(null);
            try {
              const token = await authLogin('dummy_code');
              setToken(token);
              navigate('/', { replace: true });
            } catch (error: unknown) {
              if (error instanceof ApiError) {
                setErrorMessage(
                  'Unable to sign in with dummy code: ' + error.message
                );
              } else {
                setErrorMessage(
                  'Unable to sign in with dummy code. Please try again.'
                );
              }
            }
          } else {
            login();
          }
          return {};
        }}
        slots={{ subtitle: errorMessage ? LoginAlert : undefined }}
      />
    </AppProvider>
  );
}

export default Login;
