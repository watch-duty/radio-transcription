import asyncio
import logging
import os
import sys
import time
import typing

import aiohttp
import httpx
import jwt

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _get_required_env_var(name: str) -> str:
    """Returns a required environment variable value."""
    value = os.environ.get(name)
    if not value:
        msg = f"Missing required environment variable: {name}"
        raise ValueError(msg)
    return value


class AuthenticationError(RuntimeError):
    """Raised when API authentication fails."""


class BroadcastifyOrchestrator:
    def __init__(
        self,
        username: str,
        password: str,
        api_key: str,
        api_key_id: str,
        app_id: str,
    ) -> None:
        self.username = username
        self.password = password
        self.api_key = api_key
        self.api_key_id = api_key_id
        self.app_id = app_id
        self.base_url = "https://api.bcfy.io"

        self.uid: str | None = None
        self.user_token: str | None = None
        self.last_pos: str | None = None  # Tracks position to only get new calls

    def generate_jwt(self) -> str:
        """Generates an HMAC SHA256-signed JWT."""
        iat = int(time.time())
        headers = {"alg": "HS256", "typ": "JWT", "kid": self.api_key_id}
        payload = {
            "iss": self.app_id,
            "iat": iat,
            "exp": iat + 3600,  # Token valid for 1 hour
        }

        if self.uid and self.user_token:
            payload["sub"] = int(self.uid)
            payload["utk"] = self.user_token

        return jwt.encode(payload, self.api_key, algorithm="HS256", headers=headers)

    async def authenticate(self) -> None:
        """Authenticates using HTTPX POST."""
        auth_url = f"{self.base_url}/common/v1/auth"
        token = self.generate_jwt()
        headers = {"Authorization": f"Bearer {token}"}
        data = {"username": self.username, "password": self.password}

        response = await httpx.AsyncClient().post(auth_url, headers=headers, data=data)

        if response.status_code == 200:
            res_data = response.json()
            print(res_data)
            self.uid = res_data["uid"]
            self.user_token = res_data["token"]
            logger.info(f"Authentication Successful (UID: {self.uid})")
        else:
            logger.error(f"Authentication Failed: {response.status_code}")
            raise AuthenticationError

    async def fetch_updates(
        self,
        session: aiohttp.ClientSession,
        playlist_uuid: str,
    ) -> list[dict[str, typing.Any]]:
        """Fetches new calls since self.last_pos."""
        live_url = f"{self.base_url}/calls/v1/live/"

        # Build parameters
        params = {"groups": playlist_uuid}
        if self.last_pos:
            params["pos"] = self.last_pos
        else:
            # First request: get the last 25 calls to seed the system
            params["init"] = 1
            logger.info("Initializing feed with last 25 calls...")

        token = self.generate_jwt()
        headers = {
            "Authorization": f"Bearer {token}",
        }

        async with session.get(live_url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()

                # Update the position marker
                new_pos = data.get("lastPos")
                if new_pos:
                    self.last_pos = new_pos

                calls = data.get("calls", [])
                if calls:
                    logger.info(
                        f"Received {len(calls)} new calls. lastPos updated to: {self.last_pos}"
                    )
                return calls

            if response.status == 401:
                logger.warning("JWT Expired or Token Invalid. Re-authenticating...")
                await self.authenticate()
                return []

            logger.error(f"API Error {response.status}: {await response.text()}")
            return []


async def _process_calls(
    uuid: str,
    calls: list[dict[str, typing.Any]],
) -> None:
    for call in calls:
        # Logic to hand off 'call' to your audio ingestion workers
        # e.g., queue.put_nowait(call)
        readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(call['end_ts']))
        print(f"UUID: {uuid}, Time: {readable_time}, URL: {call['url']}")


async def main() -> None:
    config = {
        "api_key": _get_required_env_var("API_KEY"),
        "api_key_id": _get_required_env_var("API_KEY_ID"),
        "app_id": _get_required_env_var("APP_ID"),
        "username": _get_required_env_var("BROADCASTIFY_USERNAME"),
        "password": _get_required_env_var("BROADCASTIFY_PASSWORD"),
    }
    uuids = ["9608-20001"]

    orchestrator = BroadcastifyOrchestrator(
        config["username"],
        config["password"],
        config["api_key"],
        config["api_key_id"],
        config["app_id"],
    )
    await orchestrator.authenticate()
    print(orchestrator.uid, orchestrator.user_token)

    # Increase connection limits for large scale ingestion
    connector = aiohttp.TCPConnector(limit=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async def _fetch_and_process(u: str) -> None:
                new_calls = await orchestrator.fetch_updates(session, u)
                await _process_calls(u, new_calls)

            tasks = [_fetch_and_process(u) for u in uuids]
            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("Orchestrator shutting down...")
        except Exception as e:
            logger.exception(f"Fatal Error: {e}")


if __name__ == "__main__":
    start_time = time.perf_counter()  # For performance monitoring
    start_time_unix = time.localtime(int(time.time()))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    end_time = time.perf_counter()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    readable_time = time.strftime('%Y-%m-%d %H:%M:%S', start_time_unix)
    logger.info(f"Current time: {readable_time}")
