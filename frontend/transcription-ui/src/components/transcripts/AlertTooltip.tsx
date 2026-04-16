import WarningAmber from '@mui/icons-material/WarningAmber';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import {useTheme} from '@mui/material/styles';

interface AlertTooltipProps {
  evaluationDecisions: string[];
  ruleIdToNameMap: Map<string, string>;
  rulesLoading: boolean;
}

export function AlertTooltip({
  evaluationDecisions,
  ruleIdToNameMap,
  rulesLoading,
}: AlertTooltipProps) {
  const theme = useTheme();
  
  if (evaluationDecisions.length == 0) {
    return null;
  }

  return (
    <Tooltip
      title={
        <Box sx={{ p: theme.spacing(2) }}>
          {evaluationDecisions.map((ruleId) => (
            <Typography
              key={ruleId}
              variant="caption"
              sx={{ display: 'block' }}
            >
              {rulesLoading ? ruleId : ruleIdToNameMap.get(ruleId) || ruleId}
            </Typography>
          ))}
        </Box>
      }
    >
      <Box
        component="span"
        sx={{ display: 'inline-flex' }}
        aria-label="view triggered rules"
      >
        <WarningAmber
          color="warning"
          fontSize="small"
          data-testid="warning-icon"
        />
      </Box>
    </Tooltip>
  );
}

export default AlertTooltip;
