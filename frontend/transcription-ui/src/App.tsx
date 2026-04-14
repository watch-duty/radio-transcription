import { Route, Routes } from 'react-router';

import AppContainer from './components/AppContainer';
import FeedsView from './components/feeds/FeedsView';
import RulesView from './components/rules/RulesView';
import TranscriptView from './components/transcripts/TranscriptView';
import { useAuth } from './context/AuthContext';

import './App.css';

function App() {
  const { token } = useAuth();

  if (!token) {
    return <AppContainer>Please login to continue.</AppContainer>;
  }

  // Define the application routes below.
  return (
    <AppContainer>
      <Routes>
        <Route path="/" element={<TranscriptView />} />
        <Route path="/rules" element={<RulesView />} />
        <Route path="/feeds" element={<FeedsView />} />
      </Routes>
    </AppContainer>
  );
}

export default App;
