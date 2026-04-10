import { GoogleLogin } from '@react-oauth/google';

import { useAuth } from '../context/AuthContext';

export function Login() {
  const { setToken } = useAuth();

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
