import LocalFireDepartmentIcon from '@material-symbols/svg-400/outlined/local_fire_department.svg?react';
import SvgIcon, { type SvgIconProps } from '@mui/material/SvgIcon';

function FireIcon(props: SvgIconProps) {
  return (
    <SvgIcon component={LocalFireDepartmentIcon} {...props} inheritViewBox />
  );
}

export default FireIcon;
