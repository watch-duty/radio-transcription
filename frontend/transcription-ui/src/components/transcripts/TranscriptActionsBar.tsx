import React from 'react';

import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import SyncIcon from '@mui/icons-material/Sync';
import { Tooltip, useTheme } from '@mui/material';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';
import CircularProgress from '@mui/material/CircularProgress';
import ClickAwayListener from '@mui/material/ClickAwayListener';
import Grow from '@mui/material/Grow';
import Link from '@mui/material/Link';
import MenuItem from '@mui/material/MenuItem';
import MenuList from '@mui/material/MenuList';
import Paper from '@mui/material/Paper';
import Popper from '@mui/material/Popper';

export interface TranscriptActionsBarProps {
  feedId: string;
  sourceUrl?: string;
  archiveUrl?: string;
  hasNewerTranscripts: boolean;
  isTranscriptsFetching: boolean;
  isTranscriptsPolling: boolean;
  refreshInterval: number;
  setRefreshInterval: (interval: number) => void;
  onRefresh: () => Promise<void>;
  triggerSnackbar: (message: string) => void;
}

const refreshIntervalOptions = [
  { value: 5000, label: '5s' },
  { value: 10000, label: '10s' },
  { value: 15000, label: '15s' },
  { value: 30000, label: '30s' },
  { value: 60000, label: '1m' },
  { value: 300000, label: '5m' },
  { value: -1, label: 'Off' },
];

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  feedId,
  sourceUrl,
  archiveUrl,
  hasNewerTranscripts,
  isTranscriptsFetching,
  isTranscriptsPolling,
  refreshInterval,
  setRefreshInterval,
  onRefresh,
  triggerSnackbar,
}) => {
  const theme = useTheme();

  const [anchorEl, setAnchorEl] = React.useState<HTMLElement | null>(null);
  const [refreshMenuOpen, setRefreshMenuOpen] = React.useState(false);

  const buttonGroupRef = React.useCallback((node: HTMLElement | null) => {
    if (node !== null) {
      setAnchorEl(node);
    }
  }, []);

  const handleToggle = () => {
    setRefreshMenuOpen((prevOpen) => !prevOpen);
  };

  const handleClose = (event: Event) => {
    if (anchorEl && anchorEl.contains(event.target as HTMLElement)) {
      return;
    }

    setRefreshMenuOpen(false);
  };

  const currentRefreshLabel = refreshIntervalOptions.find(
    (option) => option.value === refreshInterval
  )?.label;

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 1,
      }}
    >
      {sourceUrl || archiveUrl ? (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          {sourceUrl && (
            <Link
              href={sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              variant="body2"
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.5,
              }}
            >
              <LinkIcon fontSize="small" />
              Original source link
            </Link>
          )}
          {archiveUrl && (
            <Link
              href={archiveUrl}
              target="_blank"
              rel="noopener noreferrer"
              variant="body2"
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.5,
              }}
            >
              <InventoryIcon fontSize="small" />
              Archives
            </Link>
          )}
        </Box>
      ) : (
        <Box />
      )}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tooltip title="Copy link to feed">
          <Box component="span">
            <Button
              variant="outlined"
              size="small"
              disabled={!feedId}
              onClick={() => {
                if (!feedId) {
                  return;
                }

                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', feedId);
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Link copied');
              }}
              sx={{
                minWidth: 0,
                px: theme.spacing(1.5),
                textTransform: 'none',
              }}
              aria-label="copy feed deeplink"
              startIcon={<LinkIcon fontSize="small" />}
            >
              Share feed
            </Button>
          </Box>
        </Tooltip>
        {!hasNewerTranscripts && (
          <>
            <ButtonGroup variant="contained" ref={buttonGroupRef}>
              <Tooltip
                title={
                  !isTranscriptsPolling
                    ? 'Refresh transcripts'
                    : 'Refreshing transcripts...'
                }
              >
                <Button
                  size="small"
                  color="primary"
                  onClick={onRefresh}
                  disabled={isTranscriptsFetching || isTranscriptsPolling}
                  aria-label={isTranscriptsPolling ? 'refreshing' : 'refresh'}
                >
                  {isTranscriptsPolling ? (
                    <CircularProgress size={24} color="inherit" />
                  ) : (
                    <SyncIcon />
                  )}
                </Button>
              </Tooltip>
              <Tooltip title="Select refresh rate">
                <Button
                  size="small"
                  aria-controls={
                    refreshMenuOpen ? 'split-button-menu' : undefined
                  }
                  aria-expanded={refreshMenuOpen ? 'true' : undefined}
                  aria-label="select refresh interval"
                  aria-haspopup="menu"
                  onClick={handleToggle}
                  sx={{
                    textTransform: 'none',
                  }}
                >
                  {currentRefreshLabel}
                  {refreshMenuOpen ? (
                    <ArrowDropUpIcon />
                  ) : (
                    <ArrowDropDownIcon />
                  )}
                </Button>
              </Tooltip>
            </ButtonGroup>
            <Popper
              // The menu's z-index needs to be higher than the transcript view group headers.
              sx={{ zIndex: 1300 }}
              open={refreshMenuOpen}
              anchorEl={anchorEl}
              role={undefined}
              transition
              disablePortal
            >
              {({ TransitionProps, placement }) => (
                <Grow
                  {...TransitionProps}
                  style={{
                    transformOrigin:
                      placement === 'bottom' ? 'center top' : 'center bottom',
                  }}
                >
                  <Paper>
                    <ClickAwayListener onClickAway={handleClose}>
                      <MenuList id="split-button-menu" autoFocusItem>
                        {refreshIntervalOptions.map((option) => (
                          <MenuItem
                            key={option.value}
                            selected={option.value === refreshInterval}
                            onClick={() => {
                              setRefreshInterval(option.value);
                              setRefreshMenuOpen(false);
                            }}
                          >
                            {option.label}
                          </MenuItem>
                        ))}
                      </MenuList>
                    </ClickAwayListener>
                  </Paper>
                </Grow>
              )}
            </Popper>
          </>
        )}
      </Box>
    </Box>
  );
};

export default TranscriptActionsBar;
