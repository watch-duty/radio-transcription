import { useNavigate } from 'react-router';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Modal from '@mui/material/Modal';
import Typography from '@mui/material/Typography';

export type LoginModalProps = {
  open: boolean;
  setOpen: (open: boolean) => void;
};

const style = {
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
  width: 400,
  bgcolor: 'background.paper',
  boxShadow: 24,
  p: 4,
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  alignItems: 'center',
};

function LoginModal({ open, setOpen }: LoginModalProps) {
  const navigate = useNavigate();

  return (
    <Modal
      open={open}
      onClose={() => setOpen(false)}
      aria-labelledby="modal-modal-title"
      aria-describedby="modal-modal-description"
    >
      <Box sx={style}>
        <Typography variant="h6" component="h2">
          Login
        </Typography>
        <Typography>Your session has expired. Click below to login.</Typography>
        <Button
          variant="contained"
          onClick={() => {
            setOpen(false);
            navigate('/login');
          }}
        >
          Login
        </Button>
      </Box>
    </Modal>
  );
}

export default LoginModal;
