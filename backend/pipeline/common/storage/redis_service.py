import os

from redis.backoff import ExponentialBackoff
from redis.client import Redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from backend.pipeline.common.storage.cache_provider import CacheProvider

NUM_CONNECTION_RETRIES = 3
DEFAULT_REDIS_PORT = "6379"
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", DEFAULT_REDIS_PORT))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_CERTIFICATE_PATH = os.environ.get("REDIS_CERTIFICATE_PATH", "")


class RedisService(CacheProvider):
    """
    Redis service object. Used to connect to a Redis instance.
    Configured to retry on common connection errors.
    """

    def __init__(self) -> None:
        retry = Retry(ExponentialBackoff(), NUM_CONNECTION_RETRIES)
        ssl_enabled = not os.environ.get("LOCAL_DEV")
        self.client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=ssl_enabled,
            ssl_cert_reqs="required" if ssl_enabled else "none",
            ssl_ca_certs=REDIS_CERTIFICATE_PATH if ssl_enabled else None,
            db=0,
            decode_responses=True,
            retry=retry,
            retry_on_error=[
                BusyLoadingError,
                RedisConnectionError,
                RedisTimeoutError,
            ],
        )

    def set_if_not_exists(self, key: str, value: str, ttl: int) -> bool:
        """
        Set a key/value pair in the Redis instance. Returns True if the key does not exist,
        or False if does.
        """
        return bool(self.client.set(key, value, nx=True, ex=ttl))
