import React, { useMemo, useState } from 'react';

import CampaignIcon from '@mui/icons-material/Campaign';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Link from '@mui/material/Link';

export interface AnnouncementBannerProps {
  /** Start date and time when the banner should begin showing. */
  startDate: Date | string | number;
  /** End date and time when the banner should stop showing. */
  endDate: Date | string | number;
  /** Main message text to display in the banner. */
  message: string;
  /** Optional title prefix displayed before the message (e.g., "CSAT Survey:"). */
  title?: string;
  /** Optional clickable link URL to append to the banner message. */
  linkUrl?: string;
  /** Optional link display text. Defaults to linkUrl if provided. */
  linkText?: string;
  /** Optional React Node icon override for the alert banner. Defaults to CampaignIcon. */
  icon?: React.ReactNode;
  /** Optional unique storage key for tracking dismissal state in localStorage. */
  storageKey?: string;
  /** Optional date override for testing or checking display criteria. */
  currentDate?: Date;
  /** Force display ignoring dates and dismissal state. */
  forceShow?: boolean;
}

/**
 * Reusable general announcement banner component with time window gating and local dismissal tracking.
 */
export function AnnouncementBanner({
  startDate,
  endDate,
  message,
  title,
  linkUrl,
  linkText,
  icon,
  storageKey,
  currentDate,
  forceShow,
}: AnnouncementBannerProps) {
  const resolvedStorageKey = useMemo(
    () =>
      storageKey ??
      (linkUrl
        ? `announcement_banner_dismissed_${linkUrl}`
        : `announcement_banner_dismissed_${title || ''}_${startDate}`),
    [storageKey, linkUrl, title, startDate]
  );

  const [dismissed, setDismissed] = useState<boolean>(() => {
    try {
      return localStorage.getItem(resolvedStorageKey) === 'true';
    } catch {
      return false;
    }
  });

  const now = currentDate ?? new Date();
  const start = startDate instanceof Date ? startDate : new Date(startDate);
  const end = endDate instanceof Date ? endDate : new Date(endDate);

  const isWithinWindow =
    now.getTime() >= start.getTime() && now.getTime() <= end.getTime();

  const shouldRender = forceShow || (isWithinWindow && !dismissed);

  if (!shouldRender) {
    return null;
  }

  const handleClose = () => {
    setDismissed(true);
    try {
      localStorage.setItem(resolvedStorageKey, 'true');
    } catch {
      // Ignore localStorage errors in restricted browsing modes.
    }
  };

  const displayLinkText = linkText ?? linkUrl;

  return (
    <Alert
      severity="info"
      icon={icon ?? <CampaignIcon />}
      onClose={handleClose}
      sx={{
        width: '100%',
        mb: 2,
        textAlign: 'left',
        alignItems: 'center',
      }}
    >
      {title && (
        <Box component="span" sx={{ fontWeight: 500, mr: 1 }}>
          {title}
        </Box>
      )}
      {message}
      {linkUrl && (
        <Link
          href={linkUrl}
          target="_blank"
          rel="noopener noreferrer"
          underline="hover"
          sx={{ fontWeight: 600, ml: 0.5 }}
        >
          {displayLinkText}
        </Link>
      )}
    </Alert>
  );
}
