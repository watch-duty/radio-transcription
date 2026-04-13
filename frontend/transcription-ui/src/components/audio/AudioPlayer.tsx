import { useCallback, useEffect, useRef, useState } from 'react';

import { Howl } from 'howler';

import PauseIcon from '@mui/icons-material/Pause';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import IconButton from '@mui/material/IconButton';

export interface AudioPlayerProps {
  audioUri: string;
  transmissionId: string;
  onPlay: (transmissionId: string | null) => void;
  currentlyPlayingTransmissionId: string | null;
}

function AudioPlayer(props: AudioPlayerProps) {
  const { audioUri, transmissionId, currentlyPlayingTransmissionId, onPlay } =
    props;
  const [isPlaying, setIsPlaying] = useState(false);
  const sound = useRef<Howl>(null);

  // This effect initializes the audio player and ensures it is cleaned up when the component is unmounted.
  useEffect(() => {
    sound.current = new Howl({
      src: [audioUri.replace('gs://', 'https://storage.googleapis.com/')],
      html5: true,
      preload: 'metadata',
      onplay: () => { setIsPlaying(true); },
      onpause: () => { setIsPlaying(false); },
      onend: () => { setIsPlaying(false); },
      onstop: () => { setIsPlaying(false); }
    });

    return () => {
      sound.current?.unload();
    };
  }, [audioUri]);

  const stopAudio = useCallback(() => {
    sound.current?.stop();
  }, []);

  // This effect will ensure multiple audio files are not played at the same time. 
  useEffect(() => {
    if (currentlyPlayingTransmissionId !== transmissionId) {
      stopAudio();
    }
  }, [currentlyPlayingTransmissionId, transmissionId, stopAudio]);

  const toggleAudio = () => {
    if (!isPlaying) {
      onPlay(transmissionId);
      sound.current?.play();
    } else {
      sound.current?.pause();
    }
  };

  return (
    <IconButton
      onClick={toggleAudio}
      color="primary"
      aria-label={isPlaying ? 'pause' : 'play'}
    >
      {isPlaying ? <PauseIcon /> : <PlayArrowIcon />}
    </IconButton>
  );
}

export default AudioPlayer;
