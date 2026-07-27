// @vitest-environment jsdom
import { describe, expect, it, vi } from 'vitest';

import { handleListboxScroll, isNearBottom } from './scrollUtils';

describe('scrollUtils', () => {
  describe('isNearBottom', () => {
    it('returns true when scroll position is near the bottom', () => {
      const element = {
        scrollTop: 80,
        clientHeight: 100,
        scrollHeight: 200,
      } as HTMLElement;

      // 80 + 100 = 180 >= 200 - 20 = 180 -> true
      expect(isNearBottom(element)).toBe(true);
    });

    it('returns false when scroll position is not near the bottom', () => {
      const element = {
        scrollTop: 50,
        clientHeight: 100,
        scrollHeight: 200,
      } as HTMLElement;

      // 50 + 100 = 150 < 200 - 20 = 180 -> false
      expect(isNearBottom(element)).toBe(false);
    });
  });

  describe('handleListboxScroll', () => {
    it('calls onLoadMore when scrolled near bottom and hasNextPage is true', () => {
      const onLoadMore = vi.fn();
      const event = {
        currentTarget: {
          scrollTop: 80,
          clientHeight: 100,
          scrollHeight: 200,
        },
      } as unknown as React.UIEvent<HTMLUListElement>;

      handleListboxScroll(event, onLoadMore, true, false);
      expect(onLoadMore).toHaveBeenCalledTimes(1);
    });

    it('does not call onLoadMore if isFetchingNextPage is true', () => {
      const onLoadMore = vi.fn();
      const event = {
        currentTarget: {
          scrollTop: 80,
          clientHeight: 100,
          scrollHeight: 200,
        },
      } as unknown as React.UIEvent<HTMLUListElement>;

      handleListboxScroll(event, onLoadMore, true, true);
      expect(onLoadMore).not.toHaveBeenCalled();
    });

    it('does not call onLoadMore if hasNextPage is false', () => {
      const onLoadMore = vi.fn();
      const event = {
        currentTarget: {
          scrollTop: 80,
          clientHeight: 100,
          scrollHeight: 200,
        },
      } as unknown as React.UIEvent<HTMLUListElement>;

      handleListboxScroll(event, onLoadMore, false, false);
      expect(onLoadMore).not.toHaveBeenCalled();
    });
  });
});
