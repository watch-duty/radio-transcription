import React from 'react';

import CheckBoxIcon from '@mui/icons-material/CheckBox';
import CheckBoxOutlineBlankIcon from '@mui/icons-material/CheckBoxOutlineBlank';
import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import TextField from '@mui/material/TextField';

export interface MultiSelectFilterProps<T> {
  /** The full set of selectable options */
  options: T[];
  /** The currently selected values */
  value: T[];
  /** Callback triggered on value updates */
  onChange: (value: T[]) => void;
  /** Floating text label for the field */
  label: string;
  /** Size configuration */
  size?: 'small' | 'medium';
  /** How to stringify the generic option for identity/matching. Defaults to String(option). */
  getOptionLabel?: (option: T) => string;
  /** How to get a unique string identifier/value for the option. Defaults to String(option). */
  getOptionValue?: (option: T) => string;
  /** Custom comparison rule. Defaults to strict equality (===) */
  isOptionEqualToValue?: (option: T, value: T) => boolean;
  /** Grouping criteria. If defined, automatically applies the styled group header layout. */
  groupBy?: (option: T) => string;
  /** How each option text is rendered inside the dropdown menu. Defaults to String(option) */
  renderOptionContent?: (option: T) => React.ReactNode;
  /** Inner element structure for the selection indicator Chip labels. Defaults to String(option) */
  renderValueLabel?: (option: T) => React.ReactNode;
}

/**
 * A highly flexible, beautiful, type-safe multi-select dropdown filter.
 * Consolidates standard checkmark dropdown styles and responsive selection chips.
 */
export function MultiSelectFilter<T>({
  options,
  value,
  onChange,
  label,
  size,
  getOptionLabel = (option) => String(option),
  getOptionValue = (option) => String(option),
  isOptionEqualToValue = (option, val) => option === val,
  groupBy,
  renderOptionContent = (option) => String(option),
  renderValueLabel = (option) => String(option),
}: MultiSelectFilterProps<T>) {
  return (
    <Autocomplete
      multiple
      options={options}
      value={value}
      onChange={(_, newValue) => onChange(newValue)}
      disableCloseOnSelect
      getOptionLabel={getOptionLabel}
      isOptionEqualToValue={isOptionEqualToValue}
      groupBy={groupBy}
      size={size}
      renderOption={(props, option, { selected }) => {
        const { key, ...optionProps } = props;
        const SelectionIcon = selected
          ? CheckBoxIcon
          : CheckBoxOutlineBlankIcon;

        return (
          <li key={key} {...optionProps} data-value={getOptionValue(option)}>
            <SelectionIcon
              fontSize="small"
              style={{ marginRight: 8, padding: 9, boxSizing: 'content-box' }}
            />
            {renderOptionContent(option)}
          </li>
        );
      }}
      renderGroup={
        groupBy
          ? (group) => (
              <li key={group.key}>
                <Box sx={{ padding: 1, fontWeight: 'bold' }}>{group.group}</Box>
                <ul style={{ padding: 0 }}>{group.children}</ul>
              </li>
            )
          : undefined
      }
      renderInput={(params) => (
        <TextField {...params} label={label} placeholder="" size={size} />
      )}
      renderValue={(selectedValue) => (
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
          {selectedValue.map((val, idx) => (
            <Chip
              key={getOptionLabel(val) || idx}
              label={renderValueLabel(val)}
              size="small"
              variant="filled"
            />
          ))}
        </Box>
      )}
    />
  );
}

export default MultiSelectFilter;
