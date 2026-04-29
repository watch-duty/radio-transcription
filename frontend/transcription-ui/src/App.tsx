import { Suspense, lazy, useCallback, useState } from 'react';
import { Route, Routes } from 'react-router';

import Alert, { type AlertProps } from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Snackbar from '@mui/material/Snackbar';

import AppContainer from './components/AppContainer';
import Login from './components/Login';
import FeedsView from './components/feeds/FeedsView';
import RulesView from './components/rules/RulesView';
import TranscriptView from './components/transcripts/TranscriptView';
import { useAuth } from './context/AuthContext';

import './App.css';

const DocsView = lazy(() => import('./components/docs/DocsView'));

function App() {
  const { token } = useAuth();

  const [alerts, setAlerts] = useState<AlertProps[]>([]);
  const [snackbarMessage, setSnackbarMessage] = useState<string | null>(null);

  /**
   * Triggers a snackbar to display a message.
   *
   * @param message The message to display in the snackbar.
   */
  const triggerSnackbar = (message: string) => {
    setSnackbarMessage(null);
    // Typically, all updates to React state happen in the same execution loop.
    // But given the way the Snackbar component is implemented, if we were to just replace
    // the existing message, the `autoHideDuration` would not reset. That would mean
    // if it was close to closing itself and the message changed, it would only appear
    // for the remaining duration. By adding a timeout, we force the Snackbar to close
    // and then reopen with the new message on the next loop, resetting the timeout
    // duration.
    setTimeout(() => setSnackbarMessage(message), 50);
  };

  /**
   * Adds an alert to the alerts list.
   * Removes the oldest alert if the list exceeds 3 alerts.
   *
   * @param alert The alert to add to the list.
   */
  const addAlert = useCallback((alert: AlertProps) => {
    // Max of 3 alerts retained.
    setAlerts((alerts) => {
      const newAlerts = [...alerts, alert];
      if (newAlerts.length > 3) {
        newAlerts.shift();
      }
      return newAlerts;
    });
  }, []);

  if (!token) {
    return <Login />;
  }

  // Define the application routes below.
  return (
    <AppContainer>
      <Snackbar
        open={!!snackbarMessage}
        autoHideDuration={3000}
        onClose={() => setSnackbarMessage(null)}
        message={snackbarMessage}
      />
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
        <Route
          path="/"
          element={
            <TranscriptView
              addAlert={addAlert}
              triggerSnackbar={triggerSnackbar}
            />
          }
        />
        <Route path="/rules" element={<RulesView />} />
        <Route path="/feeds" element={<FeedsView />} />
        <Route
          path="/docs"
          element={
            <Suspense fallback={<div>Loading documentation...</div>}>
              <DocsView />
            </Suspense>
          }
        />
        <Route path="/login" element={<Login />} />
      </Routes>
    </AppContainer>
  );
}

export default App;
