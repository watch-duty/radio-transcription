import AddIcon from '@mui/icons-material/Add';
import CheckIcon from '@mui/icons-material/Check';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';

import { type ScannerFeed, useScanner } from '../../context/ScannerContext';

interface AddToScannerButtonProps {
  feed: ScannerFeed;
  variant?: 'icon' | 'button';
}

export function AddToScannerButton({
  feed,
  variant = 'icon',
}: AddToScannerButtonProps) {
  const { isInScanner, addFeed, removeFeed } = useScanner();
  const added = isInScanner(feed.id);

  const tooltip = added ? 'Remove from Scanner' : 'Add to Scanner';
  const handleClick = () => {
    if (added) {
      removeFeed(feed.id);
    } else {
      addFeed({ id: feed.id, name: feed.name });
    }
  };

  if (variant === 'button') {
    return (
      <Tooltip title={tooltip}>
        <Button
          variant="outlined"
          size="small"
          color={added ? 'success' : 'primary'}
          onClick={handleClick}
          sx={{ textTransform: 'none' }}
          aria-label={tooltip}
          startIcon={
            added ? (
              <CheckIcon fontSize="small" />
            ) : (
              <AddIcon fontSize="small" />
            )
          }
        >
          {added ? 'In Scanner' : 'Add to Scanner'}
        </Button>
      </Tooltip>
    );
  }

  return (
    <Tooltip title={tooltip}>
      <IconButton
        size="small"
        onClick={handleClick}
        color={added ? 'success' : 'default'}
        sx={{
          border: '1px solid',
          borderColor: added ? 'success.main' : 'divider',
          borderRadius: 1.5,
          p: 0.5,
          '&:hover': {
            borderColor: 'primary.main',
            bgcolor: 'primary.soft',
            color: 'primary.main',
          },
        }}
        aria-label={`${tooltip}: ${feed.name}`}
      >
        {added ? <CheckIcon fontSize="small" /> : <AddIcon fontSize="small" />}
      </IconButton>
    </Tooltip>
  );
}

export default AddToScannerButton;
