import { useState } from 'react';
import { Route, Routes } from 'react-router';

import Alert, { type AlertProps } from '@mui/material/Alert';
import Box from '@mui/material/Box';

import AppContainer from './components/AppContainer';
import DocsView from './components/docs/DocsView';
import FeedsView from './components/feeds/FeedsView';
import RulesView from './components/rules/RulesView';
import TranscriptView from './components/transcripts/TranscriptView';
import { useAuth } from './context/AuthContext';

import './App.css';

function App() {
  const { token } = useAuth();

  const [alerts, setAlerts] = useState<AlertProps[]>([]);

  const addAlert = (alert: AlertProps) => {
    // Max of 3 alerts retained.
    setAlerts((alerts) => {
      const newAlerts = [...alerts, alert];
      if (newAlerts.length > 3) {
        newAlerts.shift();
      }
      return newAlerts;
    });
  };

  if (!token) {
    return <AppContainer>Please login to continue.</AppContainer>;
  }

  // Define the application routes below.
  return (
    <AppContainer>
      <Box sx={{ width: '100%', mb: 2 }}>
        {alerts.map((alert, index) => (
          <Alert
            key={index}
            onClose={() =>
              setAlerts((alerts) => alerts.filter((_, i) => i !== index))
            }
            severity={alert.severity}
          >
            {alert.children}
          </Alert>
        ))}
      </Box>
      <Routes>
        <Route path="/" element={<TranscriptView addAlert={addAlert} />} />
        <Route path="/rules" element={<RulesView />} />
        <Route path="/feeds" element={<FeedsView />} />
        <Route path="/docs" element={<DocsView />} />
      </Routes>
    </AppContainer>
  );
}

export default App;
