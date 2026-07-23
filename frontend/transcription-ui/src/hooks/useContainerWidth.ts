import { useLayoutEffect, useRef, useState } from 'react';

// Tracks the rendered width of an element via ResizeObserver. Returns a ref to
// attach to the element and its last measured width in px (null before the
// first measurement). The initial read runs in a layout effect so the width is
// known before paint, avoiding a layout flash.
export function useContainerWidth<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState<number | null>(null);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    setWidth(el.getBoundingClientRect().width);
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) setWidth(entry.contentRect.width);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  return [ref, width] as const;
}
