import { useNavigate } from 'react-router';

import GoogleIcon from '@mui/icons-material/Google';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import { useGoogleLogin } from '@react-oauth/google';

import { useAuth } from '../context/AuthContext';
import { authLogin } from '../service/authLogin';

export function Login() {
  const navigate = useNavigate();
  const { setToken } = useAuth();

  const login = useGoogleLogin({
    onSuccess: async ({ code }) => {
      try {
        const token = await authLogin(code);
        setToken(token);
        // Navigate to the home screen.
        navigate('/');
      } catch (error) {
        console.error('Login failed:', error);
      }
    },
    flow: 'auth-code',
  });

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
      <Box
        sx={{
          textAlign: 'center',
          width: '20%',
          borderRadius: '4px',
          padding: '16px',
          display: 'flex',
          flexDirection: 'column',
          gap: 1,
          boxShadow:
            'rgba(0, 0, 0, 0.2) 0px 2px 4px -1px, rgba(0, 0, 0, 0.14) 0px 4px 5px 0px, rgba(0, 0, 0, 0.12) 0px 1px 10px 0px',
          border: '1px solid rgba(189, 189, 189, 0.4)',
        }}
      >
        <Typography variant="h5">
          <b>Sign in</b>
        </Typography>
        <Typography variant="subtitle1">
          Please sign in to view radio transcriptions
        </Typography>
        <Button
          variant="outlined"
          color="primary"
          startIcon={<GoogleIcon />}
          onClick={() => login()}
          sx={{
            textTransform: 'none',
            borderRadius: '4px',
            padding: '10px 24px',
            fontWeight: 600,
          }}
        >
          Sign in with Google
        </Button>
      </Box>
    </Box>
  );
}

export default Login;
