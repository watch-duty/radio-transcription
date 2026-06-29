import collections
import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_SOURCE_BCFY_CALLS = "broadcastify_calls"
DATA_SOURCE_FIRE_NOTIFICATIONS = "fire_notifications"
PATH_BCFY_CALLS = "/calls"
PATH_FIRE_NOTIFICATIONS_PREFIX = "/api/audio/"
DEFAULT_DATA_DIR = "/data"

# Pre-populated default mock feed IDs.
# Used for manual local development and as a fallback for E2E integration tests
# that generate random UUIDs for their source_feed_ids to ensure test isolation.
DEFAULT_BCFY_FEED_ID = "2912"
DEFAULT_FN_FEED_ID = "RECORDINGS/SAN-JOSE-DISP"

call_index: dict[str, int] = collections.defaultdict(int)


class RequestHandler(BaseHTTPRequestHandler):
    """
    Handles HTTP requests for the mock audio server.

    Serves mock API responses and audio files for Broadcastify Calls and
    Fire Notifications. Used during local development to emulate real audio sources
    and during automated E2E integration tests.
    """

    def _resolve_data_dir(self, data_source: str, source_feed_id: str) -> Path:
        """Resolves the data directory, falling back to defaults if necessary."""
        data_dir = Path(DEFAULT_DATA_DIR) / data_source / source_feed_id
        if data_dir.is_dir():
            return data_dir

        # Fallback for E2E integration tests.
        # Tests generate random UUID source_feed_ids to guarantee complete isolation.
        # We gracefully fall back to the pre-populated standard mock data here so tests
        # don't need to manually populate audio files for their dynamic IDs.
        if data_source == DATA_SOURCE_BCFY_CALLS:
            return Path(DEFAULT_DATA_DIR) / data_source / DEFAULT_BCFY_FEED_ID
        if data_source == DATA_SOURCE_FIRE_NOTIFICATIONS:
            return Path(DEFAULT_DATA_DIR) / data_source / DEFAULT_FN_FEED_ID
        return data_dir

    def get_audio_files(
        self, data_source: str, source_feed_id: str
    ) -> list[Path]:
        data_dir = self._resolve_data_dir(data_source, source_feed_id)
        if not data_dir.is_dir():
            return []

        valid_extensions = {".mp3", ".wav", ".flac", ".m4a", ".ogg"}
        files = [
            f
            for f in data_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions
        ]
        files.sort(key=lambda x: x.name)
        return files

    def _get_next_audio_file(
        self, parsed_url, data_source: str, source_feed_id: str
    ) -> Path | None:
        """Cycles through audio files for the given feed, returning the next one."""
        audio_files = self.get_audio_files(data_source, source_feed_id)
        if not audio_files:
            return None

        current_file = audio_files[
            call_index[parsed_url.path] % len(audio_files)
        ]
        call_index[parsed_url.path] += 1
        return current_file

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)

        if parsed_url.path in (PATH_BCFY_CALLS, f"{PATH_BCFY_CALLS}/"):
            self._handle_bcfy_calls(parsed_url)
        elif parsed_url.path.startswith(PATH_FIRE_NOTIFICATIONS_PREFIX):
            self._handle_fire_notifications(parsed_url)
        else:
            self._handle_file_download(parsed_url)

    def _handle_bcfy_calls(self, parsed_url) -> None:
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        url_base = os.environ.get("BCFY_FEEDS_URL_BASE")
        if not url_base:
            msg = "BCFY_FEEDS_URL_BASE environment variable is not set"
            raise RuntimeError(msg)

        qs = parse_qs(parsed_url.query)
        source_feed_id = qs.get("groups", [""])[0]

        current_file = self._get_next_audio_file(
            parsed_url, DATA_SOURCE_BCFY_CALLS, source_feed_id
        )

        calls = []
        if current_file:
            relative_path = current_file.relative_to(
                Path(DEFAULT_DATA_DIR)
            ).as_posix()

            calls.append(
                {
                    "id": int(time.time() * 1000) + call_index[parsed_url.path],
                    "url": f"{url_base}{relative_path}?id={int(time.time() * 1000) + call_index[parsed_url.path]}",
                    "start_ts": int(time.time()),
                    "end_ts": int(time.time()) + 5,
                    "ts": int(time.time()),
                }
            )

        response = {"calls": calls, "lastPos": int(time.time())}
        self.wfile.write(json.dumps(response).encode("utf-8"))

    def _handle_fire_notifications(self, parsed_url) -> None:
        auth_header = self.headers.get("Authorization")
        expected_auth = "Basic bW9jay11c2VyOm1vY2stcGFzc3dvcmQ="

        if auth_header != expected_auth:
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="FN API"')
            self.end_headers()
            self.wfile.write(b"Unauthorized")
            logger.warning(
                "Mock Audio Server: Unauthorized access attempt to %s",
                self.path,
            )
            return

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        source_feed_id = parsed_url.path[len(PATH_FIRE_NOTIFICATIONS_PREFIX) :]
        current_file = self._get_next_audio_file(
            parsed_url, DATA_SOURCE_FIRE_NOTIFICATIONS, source_feed_id
        )

        files_payload = []
        if current_file:
            relative_path_no_ext = (
                current_file.relative_to(Path(DEFAULT_DATA_DIR))
                .with_suffix("")
                .as_posix()
            )

            files_payload.append(
                {
                    "type": "file",
                    "name": f"{current_file.stem}.mp3?id={int(time.time() * 1000) + call_index[parsed_url.path]}",
                    "uuid": f"{relative_path_no_ext}?id={int(time.time() * 1000) + call_index[parsed_url.path]}",
                    "size": current_file.stat().st_size
                    if current_file.exists()
                    else 10240,
                }
            )

        payload = {
            "files": files_payload,
            "directories": [],
        }
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _stream_live_audio(self, file_path: Path, byte_rate: float) -> None:
        """Streams mock audio file in an infinite loop at the specified byte rate."""
        chunk_size = 4096
        logger.info(
            f"Streaming mock audio {file_path.name} in an infinite loop to simulate live stream..."
        )
        try:
            while True:
                with open(file_path, "rb") as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        # Sleep to match real-time byte rate
                        sleep_time = len(chunk) / byte_rate
                        time.sleep(sleep_time)
        except (BrokenPipeError, ConnectionResetError):
            logger.info(
                f"Client disconnected from live stream {file_path.name}."
            )

    def _handle_file_download(self, parsed_url) -> None:
        path = parsed_url.path.lstrip("/")
        path = path.removeprefix("mock-s3/")

        file_path = Path("/data") / path

        if not file_path.exists():
            for ext in [".flac", ".mp3", ".wav", ".m4a", ".ogg"]:
                alt_path = file_path.with_suffix(ext)
                if alt_path.exists():
                    file_path = alt_path
                    break

        if not file_path.exists() and "/" not in path:
            # Fallback for E2E integration tests requesting dynamic bcfy_feeds source_feed_ids.
            # Treat the root request as a continuous stream and reuse the calls mock data.
            current_file = self._get_next_audio_file(
                parsed_url, DATA_SOURCE_BCFY_CALLS, path
            )
            if current_file:
                file_path = current_file

        if file_path.exists() and file_path.is_file():
            self.send_response(200)

            content_types = {
                ".flac": "audio/flac",
                ".mp3": "audio/mpeg",
                ".wav": "audio/wav",
                ".m4a": "audio/mp4",
                ".ogg": "audio/ogg",
                ".json": "application/json",
            }
            content_type = content_types.get(
                file_path.suffix.lower(), "application/octet-stream"
            )
            self.send_header("Content-type", content_type)
            self.end_headers()

            # Only simulate live stream (throttling and holding connection open)
            # if this is a root-level request (simulating an Icecast mountpoint).
            # One-shot calls downloads (which contain slashes in the path) should be
            # served at maximum speed and closed immediately.
            is_audio = file_path.suffix.lower() in {
                ".flac",
                ".mp3",
                ".wav",
                ".m4a",
                ".ogg",
            }
            is_live_stream = is_audio and "/" not in path

            if is_live_stream:
                # Assume a standard mock duration of 15 seconds to calculate the byte rate
                duration = 15.0
                file_size = file_path.stat().st_size
                byte_rate = file_size / duration  # bytes per second
                self._stream_live_audio(file_path, byte_rate)
            else:
                with open(file_path, "rb") as f:
                    self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"File not found")


def run(port: int = 8090) -> None:
    server_address = ("0.0.0.0", port)  # noqa: S104
    httpd = ThreadingHTTPServer(server_address, RequestHandler)
    logger.info(f"Starting Mock Audio Server on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
