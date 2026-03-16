from redis.backoff import ExponentialBackoff
from redis.client import Redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

# Number of retries when connecting to the Redis instance before failing.
NUM_RETRIES = 3


class RedisService:
    """
    Redis service object. Used to connect to a Redis instance.
    Configured to retry on common connection errors.
    """

    def __init__(self, host: str, port: int, password: str | None) -> None:
        retry = Retry(ExponentialBackoff(), NUM_RETRIES)
        self.client = Redis(
            host=host,
            port=port,
            password=password,
            db=0,
            decode_responses=True,
            retry=retry,
            retry_on_error=[BusyLoadingError, RedisConnectionError, RedisTimeoutError],
        )

    def set(self, key: str, value: str, ttl_in_seconds: int) -> bool:
        """
        Set a key/value pair in the Redis instance. Returns True if the key already exists,
        or None if it does not.
        """
        return bool(self.client.set(key, value, nx=True, ex=ttl_in_seconds))
