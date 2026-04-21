import { GoogleLogin, googleLogout } from '@react-oauth/google';

import Button from '@mui/material/Button';

import { useAuth } from '../context/AuthContext';

export function Login() {
  const { token, setToken } = useAuth();

  if (token) {
    return (
      <Button
        color="inherit"
        onClick={() => {
          googleLogout();
          setToken(null);
        }}
      >
        Sign Out
      </Button>
    );
  }

  return (
    <GoogleLogin
      useOneTap={true}
      onSuccess={(credentialResponse) => {
        if (credentialResponse.credential) {
          setToken(credentialResponse.credential);
        }
      }}
      onError={() => {
        console.log('Login Failed');
      }}
    />
  );
}

export default Login;
