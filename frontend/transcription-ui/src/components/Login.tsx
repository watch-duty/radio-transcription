import { useNavigate } from 'react-router';

import { useTheme } from '@mui/material/styles';
import { useGoogleLogin } from '@react-oauth/google';
import { AppProvider } from '@toolpad/core/AppProvider';
import { SignInPage } from '@toolpad/core/SignInPage';

import { useAuth } from '../context/AuthContext';
import { authLogin } from '../service/authLogin';

export function Login() {
  const theme = useTheme();
  const navigate = useNavigate();
  const { setToken } = useAuth();

  const login = useGoogleLogin({
    onSuccess: async ({ code }) => {
      try {
        const token = await authLogin(code);
        setToken(token);
        // Navigate the user back to the previous page if it was an internal link, otherwise go to the home page.
        const isInternal = document.referrer && document.referrer.includes(window.location.host);
        if (isInternal) {
          navigate(-1);
        } else {
          navigate('/', { replace: true });
        }
      } catch (error) {
        console.error('Login failed:', error);
      }
    },
    flow: 'auth-code',
  });

  return (
    <AppProvider theme={theme}>
      <SignInPage
        providers={[{ id: 'google', name: 'Google' }]}
        signIn={login}
      />
    </AppProvider>
  );
}

export default Login;
