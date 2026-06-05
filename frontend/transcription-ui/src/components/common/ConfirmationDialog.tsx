import { useState } from 'react';

import {
  Box,
  Button,
  type ButtonProps,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  TextField,
} from '@mui/material';

export interface ConfirmationDialogProps {
  /** Controls the visibility of the dialog */
  open: boolean;
  /** The primary headline shown at the top of the dialog */
  title: string;
  /** Explanation/message displayed within the dialog box */
  description: string;
  /** Text label for the confirmation action button */
  confirmLabel: string;
  /** Color theme styling of the confirm button */
  confirmColor?: ButtonProps['color'];
  /** Variant type styling of the confirm button */
  confirmVariant?: ButtonProps['variant'];
  /** If true, requires the user to type confirmation text before enabling the action button */
  showConfirmInput?: boolean;
  /** The specific value the user needs to match (e.g., the item ID or NAME) */
  confirmInputValue?: string;
  /** Instructional text above the confirmation match text input field */
  confirmInputLabel?: string;
  /** Triggers when the cancel button is clicked or the dialog is closed */
  onClose: () => void;
  /** Triggers when the primary confirm button is clicked */
  onConfirm: () => Promise<void> | void;
  /** If true, indicates that the action is in progress and disables interaction states */
  isSubmitting?: boolean;
}

export function ConfirmationDialog({
  open,
  title,
  description,
  confirmLabel,
  confirmColor = 'primary',
  confirmVariant = 'contained',
  showConfirmInput = false,
  confirmInputValue = '',
  confirmInputLabel = '',
  onClose,
  onConfirm,
  isSubmitting = false,
}: ConfirmationDialogProps) {
  const [userInput, setUserInput] = useState('');

  const handleClose = () => {
    setUserInput('');
    onClose();
  };

  const handleConfirm = async () => {
    await onConfirm();
  };

  const isConfirmDisabled =
    isSubmitting || (showConfirmInput && userInput !== confirmInputValue);

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      aria-labelledby="confirm-dialog-title"
      aria-describedby="confirm-dialog-description"
    >
      <DialogTitle id="confirm-dialog-title">{title}</DialogTitle>
      <DialogContent>
        <DialogContentText id="confirm-dialog-description">
          {description}
        </DialogContentText>
        {showConfirmInput && (
          <Box sx={{ mt: 2 }}>
            <TextField
              fullWidth
              size="small"
              variant="outlined"
              label={confirmInputLabel}
              value={userInput}
              onChange={(e) => setUserInput(e.target.value)}
              placeholder={confirmInputValue}
              disabled={isSubmitting}
            />
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button
          onClick={handleClose}
          color="primary"
          disabled={isSubmitting}
          sx={{ textTransform: 'none' }}
        >
          Cancel
        </Button>
        <Button
          onClick={handleConfirm}
          color={confirmColor}
          variant={confirmVariant}
          disabled={isConfirmDisabled}
          sx={{ textTransform: 'none' }}
        >
          {confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
