import React from 'react';
import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';
import { useAuth } from '../../context/AuthContext';

export function DocsView() {
  const specUrl = '/openapi.json';
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
