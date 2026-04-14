import AppContainer from './components/AppContainer';
import TranscriptView from './components/transcripts/TranscriptView';
import { useAuth } from './context/AuthContext';

import './App.css';

function App() {
  const { token } = useAuth();

  return (
    <>
      <AppContainer>{token && <TranscriptView />}</AppContainer>
    </>
  );
}

export default App;
