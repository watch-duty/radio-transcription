import React from 'react';
import Switch, { type SwitchProps } from '@mui/material/Switch';
import { styled } from '@mui/material/styles';

const StyledSwitch = styled(Switch)(({ theme }) => ({
  width: 62,
  height: 34,
  padding: 7,
  '& .MuiSwitch-switchBase': {
    margin: 1,
    padding: 0,
    transform: 'translateX(6px)',
    transitionDuration: '300ms',
    '&.Mui-checked': {
      transform: 'translateX(22px)',
      '& .MuiSwitch-thumb': {
        backgroundColor: theme.palette.primary.main,
        '&::before': {
          color: theme.palette.mode === 'dark' ? theme.palette.common.black : theme.palette.common.white
        },
      },
      '& .MuiSwitch-track': {
        backgroundColor: theme.palette.mode === 'dark' ? theme.palette.common.white : theme.palette.common.black
      }
    },
  },
  '& .MuiSwitch-thumb': {
    width: 32,
    height: 32,
    boxShadow: theme.shadows[2],
    '&::before': {
      content: '"autoplay"',
      position: 'absolute',
      width: '100%',
      height: '100%',
      left: 0,
      top: 0,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      fontFamily: 'Material Symbols Outlined',
      fontSize: '20px',
      color: theme.palette.mode === 'dark' ? theme.palette.common.black : theme.palette.common.black
    },
  },
  '& .MuiSwitch-track': {
    backgroundColor: theme.palette.mode === 'dark' ? theme.palette.common.white : theme.palette.common.black,
    borderRadius: 10,
  },
}));

/**
 * A premium styled Switch that displays the Material Symbols 'autoplay' icon inside the thumb.
 * Encapsulated with a stable span wrapper using React.forwardRef to ensure perfect alignment
 * and compatibility when wrapped with MUI Tooltips or other Popper-based components.
 */
export const AutoplaySwitch = React.forwardRef<HTMLSpanElement, SwitchProps>((props, ref) => {
  const {
    checked,
    onChange,
    disabled,
    defaultChecked,
    value,
    name,
    id,
    ...restProps
  } = props;

  // Only forward defined Switch-specific props to StyledSwitch to avoid passing
  // undefined values that React 19 / Emotion could forward as custom DOM attributes.
  const switchProps: SwitchProps = {};
  if (checked !== undefined) switchProps.checked = checked;
  if (onChange !== undefined) switchProps.onChange = onChange;
  if (disabled !== undefined) switchProps.disabled = disabled;
  if (defaultChecked !== undefined) switchProps.defaultChecked = defaultChecked;
  if (value !== undefined) switchProps.value = value;
  if (name !== undefined) switchProps.name = name;
  if (id !== undefined) switchProps.id = id;

  return (
    <span
      ref={ref}
      style={{ display: 'inline-block', verticalAlign: 'middle' }}
      {...restProps}
    >
      <StyledSwitch {...switchProps} />
    </span>
  );
});

export default AutoplaySwitch;
