import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-type", "audio/flac")
        self.end_headers()

        file_path = Path("/data/test_bcfy.flac")
        if file_path.exists():
            with open(file_path, "rb") as f:
                self.wfile.write(f.read())
        else:
            self.wfile.write(b"File not found")


def run(port: int = 8090) -> None:
    server_address = ("0.0.0.0", port)  # noqa: S104
    httpd = HTTPServer(server_address, RequestHandler)
    logger.info(f"Starting Mock Icecast on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
