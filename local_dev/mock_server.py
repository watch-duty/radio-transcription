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
        post_data = self.rfile.read(content_length) if content_length > 0 else b""

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
        """Returns the list of received requests."""
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
