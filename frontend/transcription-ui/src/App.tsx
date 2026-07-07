import { Suspense, lazy, useCallback, useState } from 'react';
import { Route, Routes } from 'react-router';

import { decodeJwt } from 'jose';

import {
  CssBaseline,
  ThemeProvider,
  createTheme,
  useMediaQuery,
} from '@mui/material';
import Alert, { type AlertProps } from '@mui/material/Alert';
import AlertTitle from '@mui/material/AlertTitle';
import Snackbar from '@mui/material/Snackbar';
import Stack from '@mui/material/Stack';
import { ApiError } from '@transcription/common';

import AppContainer from './components/AppContainer';
import Login from './components/Login';
import LoginModal from './components/common/LoginModal';
import { RequireAdmin } from './components/common/RequireAdmin';
import FeedConfigurationView from './components/feeds/FeedConfigurationView';
import FeedSearchView from './components/feeds/FeedSearchView';
import RuleConfigurationView from './components/rules/RuleConfigurationView';
import DemoOutageView from './components/transcripts/DemoOutageView';
import TranscriptView from './components/transcripts/TranscriptView';
import { useAuth } from './context/AuthContext';

import './App.css';

const DocsView = lazy(() => import('./components/docs/DocsView'));

function App() {
  const { token } = useAuth();

  const [alerts, setAlerts] = useState<({ title?: string } & AlertProps)[]>([]);
  const [snackbarMessage, setSnackbarMessage] = useState<string | null>(null);
  const [loginModalOpen, setLoginModalOpen] = useState(false);

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
  const addAlert = useCallback((alert: { title?: string } & AlertProps) => {
    // Max of 3 alerts retained.
    setAlerts((alerts) => {
      const newAlerts = [...alerts, { ...alert }];
      if (newAlerts.length > 3) {
        newAlerts.shift();
      }
      return newAlerts;
    });
  }, []);

  const handleError = useCallback(
    (error: Error, titleMessage?: string) => {
      const decodedToken = token && decodeJwt(token);
      const isExpired =
        decodedToken &&
        decodedToken.exp &&
        decodedToken.exp < Date.now() / 1000;
      const appendMessage = titleMessage ? `: ${titleMessage}` : '';
      if (error instanceof ApiError) {
        switch (error.status) {
          case 401:
          case 403:
            if (isExpired) {
              addAlert({
                severity: 'error',
                title: 'Session expired',
                children: 'Please log in again.',
              });
              setLoginModalOpen(true);
              return;
            } else {
              addAlert({
                severity: 'error',
                title: 'Unauthorized' + appendMessage,
                children:
                  'You are not authorized to perform this action. Please verify your permissions.',
              });
            }
            break;
          case 404:
            addAlert({
              severity: 'error',
              title: 'Not Found' + appendMessage,
              children: 'The resource you are looking for could not be found.',
            });
            break;
          case 500:
            addAlert({
              severity: 'error',
              title: 'Server Error' + appendMessage,
              children:
                'The service is currently unavailable. Please try again later.',
            });
            break;
          default:
            addAlert({
              severity: 'error',
              title: `${error.status} Error` + appendMessage,
              children: error.message,
            });
            break;
        }
      } else {
        addAlert({
          severity: 'error',
          title: 'Unknown Error' + appendMessage,
          children: error.message,
        });
      }
    },
    [addAlert, token]
  );

  const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)');
  const theme = createTheme({
    breakpoints: {
      values: {
        xs: 0,
        sm: 768,
        md: 900,
        lg: 1200,
        xl: 1536,
      },
    },
    palette: {
      mode: prefersDarkMode ? 'dark' : 'light',
    },
    components: {
      MuiBadge: {
        styleOverrides: {
          badge: ({ ownerState, theme }) => ({
            ...(ownerState.color === 'default' && {
              backgroundColor: theme.palette.text.secondary,
              color: theme.palette.background.paper,
            }),
          }),
        },
      },
    },
  });

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {!token && window.location.pathname !== '/demo-outage' ? (
        <Login />
      ) : (
        <AppContainer>
          <LoginModal open={loginModalOpen} setOpen={setLoginModalOpen} />
          <Snackbar
            open={!!snackbarMessage}
            autoHideDuration={3000}
            onClose={() => setSnackbarMessage(null)}
            message={snackbarMessage}
          />
          {alerts.length > 0 && (
            <Stack sx={{ width: '100%', marginBottom: 1 }} spacing={1}>
              {alerts.map((alert, index) => (
                <Alert
                  key={index}
                  onClose={() =>
                    setAlerts((alerts) => alerts.filter((_, i) => i !== index))
                  }
                  severity={alert.severity}
                  sx={{
                    textAlign: 'left',
                  }}
                >
                  {alert.title && <AlertTitle>{alert.title}</AlertTitle>}
                  {alert.children}
                </Alert>
              ))}
            </Stack>
          )}
          {/* Define the application routes below. */}
          <Routes>
            <Route
              path="/"
              element={
                <>
                  <title>Radio Transcription</title>
                  <FeedSearchView
                    title="Feeds"
                    triggerSnackbar={triggerSnackbar}
                    onError={handleError}
                  />
                </>
              }
            />
            <Route
              path="/transcripts"
              element={
                <>
                  <title>Transcripts - Radio Transcription</title>
                  <TranscriptView
                    triggerSnackbar={triggerSnackbar}
                    onError={handleError}
                  />
                </>
              }
            />
            <Route
              path="/rules"
              element={
                <>
                  <title>Rules - Radio Transcription</title>
                  <RuleConfigurationView
                    triggerSnackbar={triggerSnackbar}
                    onError={handleError}
                  />
                </>
              }
            />
            <Route
              path="/feeds"
              element={
                <RequireAdmin>
                  <title>Feeds - Radio Transcription</title>
                  <FeedConfigurationView
                    triggerSnackbar={triggerSnackbar}
                    onError={handleError}
                  />
                </RequireAdmin>
              }
            />
            <Route
              path="/docs"
              element={
                <RequireAdmin>
                  <title>API Docs - Radio Transcription</title>
                  <Suspense fallback={<div>Loading documentation...</div>}>
                    <DocsView />
                  </Suspense>
                </RequireAdmin>
              }
            />
            <Route path="/demo-outage" element={<DemoOutageView />} />
            <Route path="/login" element={<Login />} />
          </Routes>
        </AppContainer>
      )}
    </ThemeProvider>
  );
}

export default App;
