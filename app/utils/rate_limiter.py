import time


class RateLimiter:
    def __init__(self, rps: float = 3.0):
        self.min_interval = 1.0 / rps
        self._last = 0.0

    def wait(self):
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.time()


rate = RateLimiter(3.0)
