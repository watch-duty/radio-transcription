import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/calls"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            url_base = os.environ.get(
                "BCFY_FEEDS_URL_BASE", "http://mock-audio-server:8090/feeds/"
            )
            response = {
                "calls": [
                    {
                        "url": f"{url_base}test-feed.mp3",
                        "start_ts": int(time.time()),
                        "end_ts": int(time.time()) + 5,
                        "ts": int(time.time()),
                    }
                ]
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))
        elif self.path.startswith("/feeds"):
            self.send_response(200)
            self.send_header("Content-type", "audio/flac")
            self.end_headers()

            file_path = Path("/data/test_bcfy.flac")
            if file_path.exists():
                with open(file_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.wfile.write(b"File not found")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")


def run(port: int = 8090) -> None:
    server_address = ("0.0.0.0", port)  # noqa: S104
    httpd = HTTPServer(server_address, RequestHandler)
    logger.info(f"Starting Mock Audio Server on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
