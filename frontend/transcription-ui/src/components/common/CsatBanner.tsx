import { useMemo, useState } from 'react';

import RateReviewIcon from '@mui/icons-material/RateReview';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Link from '@mui/material/Link';

export interface CsatBannerProps {
  /** Start date and time when the banner should begin showing. */
  startDate: Date | string | number;
  /** End date and time when the banner should stop showing. */
  endDate: Date | string | number;
  /** Survey or feedback form URL to link to. */
  formUrl: string;
  /** Optional custom title prefix displayed before the message. Defaults to "CSAT Survey:". */
  title?: string;
  /** Optional custom main message text. Defaults to standard survey notice. */
  message?: string;
  /** Optional link display text. Defaults to "${formUrl} (2 min survey)". */
  linkText?: string;
  /** Optional unique storage key for tracking dismissal state in localStorage. */
  storageKey?: string;
  /** Optional date override for testing or checking display criteria. */
  currentDate?: Date;
  /** Force display ignoring dates and dismissal state. */
  forceShow?: boolean;
}

/**
 * Reusable survey/CSAT banner component with time window gating and local dismissal tracking.
 */
export function CsatBanner({
  startDate,
  endDate,
  formUrl,
  title = 'CSAT Survey:',
  message = 'Please share your experience of this transcription tool by Wed, July 29!',
  linkText,
  storageKey,
  currentDate,
  forceShow,
}: CsatBannerProps) {
  const resolvedStorageKey = useMemo(
    () => storageKey ?? `survey_banner_dismissed_${formUrl}`,
    [storageKey, formUrl]
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

  const displayLinkText = linkText ?? `${formUrl} (2 min survey)`;

  return (
    <Alert
      severity="info"
      icon={<RateReviewIcon />}
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
      {message}{' '}
      <Link
        href={formUrl}
        target="_blank"
        rel="noopener noreferrer"
        underline="hover"
        sx={{ fontWeight: 600, ml: 0.5 }}
      >
        {displayLinkText}
      </Link>
    </Alert>
  );
}
