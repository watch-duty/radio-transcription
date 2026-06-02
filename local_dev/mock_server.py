"""Mock server module for testing the notification pipeline locally."""

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tracking requests for integration tests
_received_requests = []


class RequestHandler(BaseHTTPRequestHandler):
    """Handles HTTP requests for the mock server."""

    def do_POST(self) -> None:
        """Processes incoming POST requests and echoes the JSON payload."""
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = (
            self.rfile.read(content_length) if content_length > 0 else b""
        )

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # Parse the incoming JSON, default to string if not JSON
        try:
            parsed_data = json.loads(post_data.decode("utf-8"))
        except json.JSONDecodeError:
            parsed_data = post_data.decode("utf-8")

        _received_requests.append(parsed_data)

        response = {"message": "Success", "received_data": parsed_data}
        self.wfile.write(json.dumps(response).encode("utf-8"))
        logger.info(
            "Mock Server received POST request with data:\n%s", parsed_data
        )

    def do_GET(self) -> None:
        """Returns the list of received requests or mocks FN API."""
        if self.path.startswith("/api/audio/"):
            auth_header = self.headers.get("Authorization")
            expected_auth = "Basic bW9jay11c2VyOm1vY2stcGFzc3dvcmQ="

            if auth_header != expected_auth:
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="FN API"')
                self.end_headers()
                self.wfile.write(b"Unauthorized")
                logger.warning(
                    "Mock Server: Unauthorized access attempt to %s", self.path
                )
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            # Generate a consistent mock file.
            mock_uuid = "mock-uuid-1234"
            mock_name = "MOCK-DISP 2026-05-20 12-00-00.mp3"

            payload = {
                "files": [
                    {
                        "type": "file",
                        "name": mock_name,
                        "uuid": mock_uuid,
                        "size": 10240,
                    }
                ],
                "directories": [],
            }
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        if self.path.startswith("/mock-s3/") and self.path.endswith(".mp3"):
            self.send_response(200)
            self.send_header("Content-type", "audio/mpeg")
            self.end_headers()
            # Send dummy bytes for the audio file
            self.wfile.write(b"\0" * 1024)
            return

        # Default GET behavior for tracking
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(_received_requests).encode("utf-8"))

    def do_DELETE(self) -> None:
        """Clears the list of received requests."""
        _received_requests.clear()
        self.send_response(200)
        self.end_headers()


def run(
    server_class: type[HTTPServer] = HTTPServer,
    handler_class: type[RequestHandler] = RequestHandler,
    port: int = 8082,
) -> None:
    """Starts the mock server on the specified port.

    Args:
        server_class: The HTTP server class to use.
        handler_class: The request handler class.
        port: The port number to listen on.
    """
    # This is safe to ignore because this server is only used for local dev.
    server_address = ("0.0.0.0", port)  # noqa: S104
    httpd = server_class(server_address, handler_class)
    logger.info("Starting Mock Server on port %s...", port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    run()
