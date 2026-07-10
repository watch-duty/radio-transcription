import { type MouseEvent, useState } from 'react';

// Backs a filter popover whose edits are a draft that only reach the committed
// value on Apply (Cancel/close discard it). Shared by the search and date
// filters so their open/close/apply/clear semantics can't drift.
export interface DraftPopover<T> {
  anchorEl: HTMLElement | null;
  isOpen: boolean;
  draft: T;
  setDraft: (value: T) => void;
  open: (event: MouseEvent<HTMLElement>) => void;
  cancel: () => void;
  apply: () => void;
  clear: () => void;
}

export function useDraftPopover<T>(
  committed: T,
  commit: (value: T) => void,
  emptyValue: T
): DraftPopover<T> {
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [draft, setDraft] = useState<T>(committed);

  // Mirror external commits into the draft during render, not in an effect.
  const [prevCommitted, setPrevCommitted] = useState<T>(committed);
  if (committed !== prevCommitted) {
    setPrevCommitted(committed);
    setDraft(committed);
  }

  return {
    anchorEl,
    isOpen: Boolean(anchorEl),
    draft,
    setDraft,
    open: (event) => {
      setAnchorEl(event.currentTarget);
      setDraft(committed);
    },
    cancel: () => {
      setAnchorEl(null);
      setDraft(committed);
    },
    apply: () => {
      setAnchorEl(null);
      commit(draft);
    },
    clear: () => {
      setAnchorEl(null);
      setDraft(emptyValue);
      commit(emptyValue);
    },
  };
}
