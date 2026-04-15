import SwaggerUI from 'swagger-ui-react';

import { useAuth } from '../../context/AuthContext';

import 'swagger-ui-react/swagger-ui.css';

export function DocsView() {
  const specUrl = '/api/v1/docs/openapi.json';
  const { token } = useAuth();

  return (
    <div style={{ textAlign: 'left' }}>
      <SwaggerUI
        url={specUrl}
        requestInterceptor={(req) => {
          if (token) {
            req.headers['Authorization'] = `Bearer ${token}`;
          }
          return req;
        }}
      />
    </div>
  );
}

export default DocsView;
