import React, { useState } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router';

import AppRegistrationIcon from '@mui/icons-material/AppRegistration';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import DescriptionIcon from '@mui/icons-material/Description';
import MenuIcon from '@mui/icons-material/Menu';
import RuleIcon from '@mui/icons-material/Rule';
import SettingsIcon from '@mui/icons-material/Settings';
import TroubleshootIcon from '@mui/icons-material/Troubleshoot';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import { styled, useTheme } from '@mui/material/styles';

import { useAuth } from '../context/AuthContext';
import { authLogout } from '../service/authLogout';

const drawerWidth = 240;

const DrawerHeader = styled('div')(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  padding: theme.spacing(0, 1),
  // necessary for content to be below app bar
  ...theme.mixins.toolbar,
  justifyContent: 'flex-end',
}));

export default function AppContainer({
  children,
}: {
  children: React.ReactNode;
}) {
  const navigate = useNavigate();
  const theme = useTheme();
  const { token, setToken, isAdmin } = useAuth();
  const [open, setOpen] = useState(false);

  const handleDrawerOpen = () => {
    setOpen(true);
  };

  const handleDrawerClose = () => {
    setOpen(false);
  };

  const handleItemClick = (path: string) => {
    navigate(path);
    handleDrawerClose();
  };

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed">
        <Toolbar>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            onClick={handleDrawerOpen}
            edge="start"
            sx={{ mr: 2 }}
          >
            <MenuIcon />
          </IconButton>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ flexGrow: 1, textAlign: 'left' }}
          >
            <Link
              component={RouterLink}
              to="/"
              color="inherit"
              sx={{
                textDecoration: 'none',
              }}
            >
              Radio Transcription
            </Link>
          </Typography>
          {token && (
            <Button
              onClick={async () => {
                await authLogout();
                setToken(null);
                navigate('/login');
              }}
              color="inherit"
              sx={{
                textTransform: 'none',
              }}
            >
              Sign out
            </Button>
          )}
        </Toolbar>
      </AppBar>
      <Drawer
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
          },
        }}
        variant="temporary"
        anchor="left"
        open={open}
        onClose={handleDrawerClose}
      >
        <DrawerHeader>
          <IconButton onClick={handleDrawerClose}>
            {theme.direction === 'ltr' ? (
              <ChevronLeftIcon />
            ) : (
              <ChevronRightIcon />
            )}
          </IconButton>
        </DrawerHeader>
        <Divider />
        <List>
          <ListItem disablePadding>
            <ListItemButton onClick={() => handleItemClick('/')}>
              <ListItemIcon>
                <TroubleshootIcon />
              </ListItemIcon>
              <ListItemText primary={'Feeds'} />
            </ListItemButton>
          </ListItem>
          <ListItem disablePadding>
            <ListItemButton onClick={() => handleItemClick('/rules')}>
              <ListItemIcon>
                <RuleIcon />
              </ListItemIcon>
              <ListItemText primary={'Rules'} />
            </ListItemButton>
          </ListItem>
          {isAdmin && (
            <ListItem disablePadding>
              <ListItemButton onClick={() => handleItemClick('/feeds')}>
                <ListItemIcon>
                  <AppRegistrationIcon />
                </ListItemIcon>
                <ListItemText primary={'Feed Configuration'} />
              </ListItemButton>
            </ListItem>
          )}
          {isAdmin && (
            <ListItem disablePadding>
              <ListItemButton onClick={() => handleItemClick('/docs')}>
                <ListItemIcon>
                  <DescriptionIcon />
                </ListItemIcon>
                <ListItemText primary={'API Docs'} />
              </ListItemButton>
            </ListItem>
          )}
        </List>
        <Divider />
        <List>
          <ListItem disablePadding>
            <ListItemButton onClick={() => handleItemClick('/settings')}>
              <ListItemIcon>
                <SettingsIcon />
              </ListItemIcon>
              <ListItemText primary={'Settings'} />
            </ListItemButton>
          </ListItem>
        </List>
      </Drawer>
      {/* The styling here ensures our main container fills the entire webpage. This allows all children to adapt to the full screen size. */}
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: { xs: 1.5, sm: 3 },
          height: '100vh',
          display: 'flex',
          flexDirection: 'column',
          boxSizing: 'border-box',
          overflowY: 'auto',
        }}
      >
        <DrawerHeader />
        {children}
      </Box>
    </Box>
  );
}
