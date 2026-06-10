import type { Plugin } from 'vite';

// Mock Authentication Plugin (enabled if VITE_AUTH_BACKEND === 'none')
export const mockAuthPlugin: Plugin = {
  name: 'mock-auth',
  configureServer(server) {
    server.middlewares.use((req, res, next) => {
      if (
        req.url === '/api/v1/auth/google' ||
        req.url === '/api/v1/auth/session'
      ) {
        const header = Buffer.from(
          JSON.stringify({ alg: 'none', typ: 'JWT' })
        ).toString('base64url');
        const payload = Buffer.from(
          JSON.stringify({
            email: 'local-dev@example.com',
            email_verified: true,
            sub: 'local-dev',
            aud: 'local-dev-aud',
            iss: 'https://accounts.google.com',
            exp: Math.floor(Date.now() / 1000) + 24 * 60 * 60,
          })
        ).toString('base64url');
        res.setHeader('Content-Type', 'application/json');
        res.end(
          JSON.stringify({
            idToken: `${header}.${payload}.mocksignature`,
          })
        );
      } else {
        next();
      }
    });
  },
};
