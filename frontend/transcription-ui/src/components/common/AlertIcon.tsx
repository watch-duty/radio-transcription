import Icon, { type IconProps } from '@mui/material/Icon';

interface AlertIconProps extends IconProps {
  /**
   * The Material Symbol name (ligature) to display.
   * e.g., 'local_fire_department', 'warning', 'notifications_active'
   * https://fonts.google.com/icons?icon.set=Material+Symbols&icon.size=24&icon.color=%231f1f1f&icon.platform=web
   */
  iconName: string;
}

export function AlertIcon({ iconName, ...props }: AlertIconProps) {
  return (
    <Icon baseClassName="material-symbols-outlined" {...props}>
      {iconName}
    </Icon>
  );
}

const ICON_NAME = import.meta.env.VITE_ALERT_ICON_SYMBOL_NAME || 'warning';

export function CustomAlertIcon(props: IconProps) {
  return <AlertIcon iconName={ICON_NAME} {...props} />;
}
