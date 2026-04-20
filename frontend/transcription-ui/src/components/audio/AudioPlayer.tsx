import { useCallback, useEffect, useRef, useState } from 'react';

import { Howl } from 'howler';

import PauseIcon from '@mui/icons-material/Pause';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import IconButton from '@mui/material/IconButton';
import { getAudioUrl } from '../../utils/audioUtils';

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

  // Cleanup effect to ensure sound is unloaded when component unmounts
  useEffect(() => {
    return () => {
      sound.current?.unload();
    };
  }, []);

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
    if (!sound.current) {
      sound.current = new Howl({
        src: [getAudioUrl(audioUri)],
        html5: true,
        preload: 'metadata',
        onplay: () => setIsPlaying(true),
        onpause: () => setIsPlaying(false),
        onend: () => setIsPlaying(false),
        onstop: () => setIsPlaying(false),
      });
    }

    if (!isPlaying) {
      onPlay(transmissionId);
      sound.current.play();
    } else {
      sound.current.pause();
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
