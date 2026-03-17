"""Mock server module for testing the notification pipeline locally."""

import json
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer


class RequestHandler(BaseHTTPRequestHandler):
    """Handles HTTP requests for the mock server."""
    def do_POST(self):
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
        print(f"Mock Server received POST request with data:\n{parsed_data}")


def run(server_class=HTTPServer, handler_class=RequestHandler, port=8082):
    """Starts the mock server on the specified port.

    Args:
        server_class: The HTTP server class to use.
        handler_class: The request handler class.
        port: The port number to listen on.
    """
    server_address = ("0.0.0.0", port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting Mock Server on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    run()
