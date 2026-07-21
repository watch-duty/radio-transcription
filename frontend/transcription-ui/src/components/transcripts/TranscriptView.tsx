import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';

import Box from '@mui/material/Box';
import { useTheme } from '@mui/material/styles';
import { useQuery } from '@tanstack/react-query';
import { type Feed } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import FeedPanel from '../feeds/FeedPanel';
import FeedSearchView from '../feeds/FeedSearchView';
import FeedHeader from './FeedHeader';

interface TranscriptViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FEED_POLLING_INTERVAL_MS = 15000; // 15 seconds

export function TranscriptView({
  triggerSnackbar,
  onError,
}: TranscriptViewProps) {
  const { token } = useAuth();
  useTheme();

  const [searchParams, setSearchParams] = useSearchParams();
  const targetFeedId = searchParams.get('feedId');
  const targetSegmentId = searchParams.get('segmentId');
  const targetTimestampParam = searchParams.get('timestamp');

  // Need to memoize the timestamp since Dates are compared by object reference.
  const targetTimestamp = useMemo(
    () =>
      targetTimestampParam ? new Date(Number(targetTimestampParam)) : null,
    [targetTimestampParam]
  );

  const searchedFeedId = targetFeedId || '';

  const [newMessageCount, setNewMessageCount] = useState(0);

  const {
    data: feedsData,
    error: feedsError,
    isSuccess: isFeedsSuccess,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  const feeds = useMemo(() => feedsData?.feeds || [], [feedsData]);

  const { data: activeFeedData } = useQuery({
    queryKey: ['getFeed', token, searchedFeedId],
    queryFn: () => getFeed(searchedFeedId, token!),
    enabled: !!token && !!searchedFeedId,
    refetchInterval: FEED_POLLING_INTERVAL_MS,
    refetchOnWindowFocus: true,
  });

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  // Memoizing the feed ID to feed map so we don't have to recreate it on every render.
  const feedIdToFeedMap = useMemo(() => {
    if (!feeds) {
      return new Map<string, NonNullable<typeof feeds>[number]>();
    }
    return new Map(feeds.map((f) => [f.id, f]));
  }, [feeds]);

  const searchedFeed = feedIdToFeedMap.get(searchedFeedId) || null;

  useEffect(() => {
    if (!searchedFeed) return;

    let pageTitle = `${searchedFeed.name} - Radio Transcription`;
    if (newMessageCount > 0) {
      pageTitle = `(${newMessageCount}) ${pageTitle}`;
    }
    if (document.title !== pageTitle) {
      document.title = pageTitle;
    }
  }, [searchedFeed, newMessageCount]);

  // Clear the unread message indicator when the user focuses back on the page
  useEffect(() => {
    const handleFocus = () => {
      setNewMessageCount(0);
    };

    window.addEventListener('focus', handleFocus);
    return () => {
      window.removeEventListener('focus', handleFocus);
    };
  }, []);

  const handleNewSpeechSegments = useCallback((count: number) => {
    if (!document.hasFocus()) {
      setNewMessageCount((prevCount) => prevCount + count);
    }
  }, []);

  const {
    data: rules,
    error: rulesError,
    isLoading: rulesLoading,
  } = useQuery({
    queryKey: ['listRules', token],
    queryFn: () => listRules(token ?? ''),
    enabled: !!token && isFeedsSuccess,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (rulesError) {
      onError(rulesError, 'Loading rules');
    }
  }, [rulesError, onError]);

  // Memoizing the rule ID to name map so we don't have to recreate it on every render.
  const ruleIdToNameMap: Map<string, string> = useMemo(() => {
    if (!rules) {
      return new Map<string, string>();
    }
    return new Map(rules.map((rule) => [rule.ruleId, rule.ruleName]));
  }, [rules]);

  const handleDateTimeChange = useCallback(
    (date: Date | null) => {
      setSearchParams((prev) => {
        if (date) {
          prev.set('timestamp', date.getTime().toString());
        } else {
          prev.delete('timestamp');
        }
        return prev;
      });
    },
    [setSearchParams]
  );

  const handleFeedSelect = (feed: Feed) => {
    setNewMessageCount(0);
    // A fresh feed clears the deep-link params; FeedPanel remounts on the new
    // feedId (see the key below), resetting its playback and view state.
    setSearchParams((prev) => {
      prev.set('feedId', feed.id);
      prev.delete('segmentId');
      prev.delete('timestamp');
      return prev;
    });
  };

  if (!token) {
    return null;
  }

  if (!searchedFeedId) {
    return (
      <FeedSearchView
        title="Select a feed to view transcripts"
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />
    );
  }

  const customSourceUrl = searchedFeed?.tags?.find(
    (t) => t.key === 'source_url'
  )?.value;
  const customArchiveUrl = searchedFeed?.tags?.find(
    (t) => t.key === 'archive_url'
  )?.value;
  const sourceUrl = customSourceUrl || searchedFeed?.sourceUrl;
  const archiveUrl = customArchiveUrl || searchedFeed?.archiveUrl;

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        height: 'calc(100vh)',
      }}
    >
      <FeedHeader
        searchedFeed={searchedFeed}
        onSelectFeed={handleFeedSelect}
        sourceUrl={sourceUrl}
        archiveUrl={archiveUrl}
        status={activeFeedData?.status ?? searchedFeed?.status}
        lastSpeechSegmentTimestamp={activeFeedData?.lastSpeechSegmentTimestamp}
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />

      <FeedPanel
        key={searchedFeedId}
        feed={searchedFeed}
        feedId={searchedFeedId}
        token={token}
        ruleIdToNameMap={ruleIdToNameMap}
        rulesLoading={rulesLoading}
        triggerSnackbar={triggerSnackbar}
        dateTime={targetTimestamp}
        onDateTimeChange={handleDateTimeChange}
        targetSegmentId={targetSegmentId}
        isReady={isFeedsSuccess}
        onNewSpeechSegments={handleNewSpeechSegments}
      />
    </Box>
  );
}

export default TranscriptView;
