"""Mock server module for testing the notification pipeline locally."""

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        except Exception:
            parsed_data = post_data.decode("utf-8")

        response = {
            "message": "Success",
            "received_data": parsed_data
        }
        self.wfile.write(json.dumps(response).encode("utf-8"))
        logger.info("Mock Server received POST request with data:\n%s", parsed_data)


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
    server_address = ("localhost", port)
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
