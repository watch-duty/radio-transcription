import time


class RateLimiter:
    def __init__(self, qps):
        self._min_interval = 1.0 / qps
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last = time.monotonic()
