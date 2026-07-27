import type React from 'react';

/**
 * Distance in pixels from the bottom of a scrollable container to trigger infinite loading.
 */
export const LISTBOX_SCROLL_THRESHOLD_PX = 20;

/**
 * Returns true if a scrollable HTML element is within `thresholdPx` pixels of the bottom.
 */
export function isNearBottom(
  element: HTMLElement,
  thresholdPx = LISTBOX_SCROLL_THRESHOLD_PX
): boolean {
  return (
    element.scrollTop + element.clientHeight >=
    element.scrollHeight - thresholdPx
  );
}

/**
 * Scroll event handler for MUI Autocomplete listbox slots to trigger infinite page loading.
 */
export function handleListboxScroll(
  event: React.UIEvent<HTMLUListElement>,
  onLoadMore?: () => void,
  hasNextPage?: boolean,
  isFetchingNextPage?: boolean,
  thresholdPx = LISTBOX_SCROLL_THRESHOLD_PX
): void {
  const listboxNode = event.currentTarget;
  if (
    isNearBottom(listboxNode, thresholdPx) &&
    hasNextPage &&
    !isFetchingNextPage &&
    onLoadMore
  ) {
    onLoadMore();
  }
}
