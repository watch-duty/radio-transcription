import { useState } from 'react';

import Box from '@mui/material/Box';
import Container from '@mui/material/Container';
import List from '@mui/material/List';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { AnnotationType, AudioClassification } from '@transcription/common';

import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import AudioDisplay from '../audio/AudioDisplay';
import TranscriptRow from './TranscriptRow';

const mockSegments: RenderableAudioSegment[] = [
  {
    id: 'seg-4',
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: '2026-06-29T20:00:25Z',
    endTimestamp: '2026-06-29T20:00:30Z',
    playbackAudioUri: '',
    canonicalAudioUri: '',
    missingPriorContext: true,
    missingPostContext: true,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:25Z',
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: '2026-06-29T20:00:25Z',
        data: {
          text: 'This segment is extremely degraded because it is missing both prior and post context.',
          errors: [],
        },
      },
    ],
  },
  {
    id: 'seg-3',
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: '2026-06-29T20:00:20Z',
    endTimestamp: '2026-06-29T20:00:25Z',
    playbackAudioUri: '',
    canonicalAudioUri: '',
    missingPriorContext: false,
    missingPostContext: true,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:20Z',
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: '2026-06-29T20:00:20Z',
        data: {
          text: 'This segment is missing post context. The end might be cut off abruptly.',
          errors: [],
        },
      },
    ],
  },
  {
    id: 'outage-1',
    feedId: 'feed-1',
    classification: AudioClassification.UNSPECIFIED,
    startTimestamp: '2026-06-29T20:00:10Z',
    endTimestamp: '2026-06-29T20:00:20Z',
    playbackAudioUri: '',
    canonicalAudioUri: '',
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:10Z',
    annotations: [],
    isOutageBundle: true,
  },
  {
    id: 'seg-2',
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: '2026-06-29T20:00:05Z',
    endTimestamp: '2026-06-29T20:00:10Z',
    playbackAudioUri: '',
    canonicalAudioUri: '',
    missingPriorContext: true,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:05Z',
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: '2026-06-29T20:00:05Z',
        data: {
          text: 'This segment is missing prior context. The beginning might be cut off.',
          errors: [],
        },
      },
    ],
  },
  {
    id: 'seg-1',
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: '2026-06-29T20:00:00Z',
    endTimestamp: '2026-06-29T20:00:05Z',
    playbackAudioUri: '',
    canonicalAudioUri: '',
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:00Z',
    annotations: [
      {
        type: AnnotationType.TRANSCRIPT,
        createdAt: '2026-06-29T20:00:00Z',
        data: {
          text: 'This is a normal transcription segment with full context.',
          errors: [],
        },
      },
    ],
  },
];

export function DemoOutageView() {
  const [highlightedId, setHighlightedId] = useState<string | null>(null);

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Box sx={{ mb: 4 }}>
        <Typography
          variant="h4"
          component="h1"
          gutterBottom
          sx={{ fontWeight: 'bold' }}
        >
          Audio Outage & Missing Context Demo
        </Typography>
        <Typography variant="body1" color="text.secondary">
          This page demonstrates the visual indicators for physical audio
          outages (gaps) and the new warning indicators for speech segments that
          are missing prior or post context (which may cause degraded
          transcription).
        </Typography>
      </Box>

      <Paper
        variant="outlined"
        sx={{ p: 2, mb: 3, bgcolor: 'background.default' }}
      >
        <Typography
          variant="subtitle2"
          gutterBottom
          sx={{ fontWeight: 'bold', color: 'text.secondary' }}
        >
          TIMELINE VIEW (AudioDisplay)
        </Typography>
        <Typography
          variant="caption"
          sx={{ display: 'block', mb: 2, color: 'text.secondary' }}
        >
          Note the pulsing grey block representing the physical outage (10s gap)
          on the timeline, and the warning indicators in the transcript list
          below.
        </Typography>
        <AudioDisplay
          audioSegments={mockSegments}
          currentlyPlayingSegmentId={null}
          highlightedSegmentId={highlightedId}
          onClipClick={(id) => setHighlightedId(id)}
          isAudioPlaying={false}
          onTogglePlayPause={() => {}}
        />
      </Paper>

      <Paper variant="outlined" sx={{ p: 0 }}>
        <Box sx={{ p: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
          <Typography
            variant="subtitle2"
            sx={{ fontWeight: 'bold', color: 'text.secondary' }}
          >
            TRANSCRIPT LIST (TranscriptRow)
          </Typography>
        </Box>
        <List disablePadding>
          {mockSegments.map((segment, index) => (
            <TranscriptRow
              key={segment.id}
              audioSegment={segment}
              index={index}
              totalAudioSegments={mockSegments.length}
              ruleIdToNameMap={new Map()}
              rulesLoading={false}
              onToggleAudio={() => {}}
              isAudioPlaying={false}
              currentlyPlayingSegmentId={null}
              triggerSnackbar={() => {}}
              showHeader={index === 0}
              isHighlighted={highlightedId === segment.id}
              onRowClick={(id) => setHighlightedId(id)}
            />
          ))}
        </List>
      </Paper>
    </Container>
  );
}

export default DemoOutageView;
