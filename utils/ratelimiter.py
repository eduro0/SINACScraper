import time
import asyncio

class RateLimiter:
    def __init__(self, requests_per_second):
        self.requests_per_second = requests_per_second
        self.last_request_time = 0
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            time_since_last_request = time.time() - self.last_request_time
            if time_since_last_request < 1 / self.requests_per_second:
                await asyncio.sleep(1 / self.requests_per_second - time_since_last_request)
            self.last_request_time = time.time()