import { useState } from 'react';

import WarningAmber from '@mui/icons-material/WarningAmber';
import Box from '@mui/material/Box';
import ClickAwayListener from '@mui/material/ClickAwayListener';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';

interface AlertTooltipProps {
  evaluationDecisions: string[];
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
}

export function AlertTooltip({ evaluationDecisions, ruleIdToNameMap, rulesLoading }: AlertTooltipProps) {
  const [open, setOpen] = useState(false);

  if (!evaluationDecisions || evaluationDecisions.length === 0) {
    return null;
  }

  return (
    <ClickAwayListener onClickAway={() => setOpen(false)}>
      <span>
        <Tooltip
          title={
            <Box sx={{ p: 0.5 }}>
              {evaluationDecisions.map((ruleId) => (
                <Typography key={ruleId} variant="caption" sx={{ display: 'block' }}>
                  {rulesLoading ? ruleId : (ruleIdToNameMap.get(ruleId) || ruleId)}
                </Typography>
              ))}
            </Box>
          }
          open={open}
          onClose={() => setOpen(false)}
          disableHoverListener
          disableFocusListener
          disableTouchListener
        >
          <Box
            component="span"
            sx={{ display: 'inline-flex', cursor: 'pointer' }}
            onClick={(e) => {
              e.stopPropagation();
              setOpen(!open);
            }}
            aria-label="view triggered rules"
          >
            <WarningAmber color="warning" fontSize="small" data-testid="warning-icon" />
          </Box>
        </Tooltip>
      </span>
    </ClickAwayListener>
  );
}

export default AlertTooltip;
