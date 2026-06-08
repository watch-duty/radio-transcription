import json
import logging
import os
import time
import collections
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

call_index: dict[str, int] = collections.defaultdict(int)

class RequestHandler(BaseHTTPRequestHandler):
    def get_audio_files(self, data_source: str, source_feed_id: str) -> list[Path]:
        data_dir = Path("/data") / data_source / source_feed_id
        if not data_dir.exists() or not data_dir.is_dir():
            return []
        
        valid_extensions = {".mp3", ".wav", ".flac", ".m4a", ".ogg"}
        files = [f for f in data_dir.iterdir() if f.is_file() and f.suffix.lower() in valid_extensions]
        files.sort(key=lambda x: x.name)
        return files

    def do_GET(self) -> None:
        global call_index
        parsed_url = urlparse(self.path)
        
        if parsed_url.path in ("/calls", "/calls/"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            url_base = os.environ.get(
                "BCFY_FEEDS_URL_BASE", "http://mock-audio-server:8090/"
            )
            
            qs = parse_qs(parsed_url.query)
            source_feed_id = qs.get("groups", [""])[0]
            
            audio_files = self.get_audio_files("broadcastify_calls", source_feed_id)
            
            calls = []
            if audio_files:
                current_file = audio_files[call_index[self.path] % len(audio_files)]
                call_index[self.path] += 1
                
                calls.append({
                    "url": f"{url_base}broadcastify_calls/{source_feed_id}/{current_file.name}",
                    "start_ts": int(time.time()),
                    "end_ts": int(time.time()) + 5,
                    "ts": int(time.time()),
                })

            response = {
                "calls": calls
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))
        elif self.path.startswith("/api/audio/"):
            auth_header = self.headers.get("Authorization")
            expected_auth = "Basic bW9jay11c2VyOm1vY2stcGFzc3dvcmQ="

            if auth_header != expected_auth:
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="FN API"')
                self.end_headers()
                self.wfile.write(b"Unauthorized")
                logger.warning(
                    "Mock Audio Server: Unauthorized access attempt to %s", self.path
                )
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()

            source_feed_id = self.path[len("/api/audio/"):]
            audio_files = self.get_audio_files("fire_notifications", source_feed_id)
            
            files_payload = []
            if audio_files:
                current_file = audio_files[call_index[self.path] % len(audio_files)]
                call_index[self.path] += 1
                
                files_payload.append({
                    "type": "file",
                    "name": f"{current_file.stem}.mp3",
                    "uuid": f"fire_notifications/{source_feed_id}/{current_file.stem}",
                    "size": current_file.stat().st_size if current_file.exists() else 10240,
                })

            payload = {
                "files": files_payload,
                "directories": [],
            }
            self.wfile.write(json.dumps(payload).encode("utf-8"))
        else:
            path = self.path.lstrip('/')
            if path.startswith("mock-s3/"):
                path = path[len("mock-s3/"):]
                
            file_path = Path("/data") / path
            
            if not file_path.exists():
                for ext in [".flac", ".mp3", ".wav", ".m4a", ".ogg"]:
                    alt_path = file_path.with_suffix(ext)
                    if alt_path.exists():
                        file_path = alt_path
                        break
            
            filename = file_path.name
            
            if file_path.exists() and file_path.is_file():
                self.send_response(200)
                if filename.endswith(".flac"):
                    self.send_header("Content-type", "audio/flac")
                elif filename.endswith(".mp3"):
                    self.send_header("Content-type", "audio/mpeg")
                elif filename.endswith(".wav"):
                    self.send_header("Content-type", "audio/wav")
                else:
                    self.send_header("Content-type", "application/octet-stream")
                self.end_headers()

                with open(file_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"File not found")


def run(port: int = 8090) -> None:
    server_address = ("0.0.0.0", port)  # noqa: S104
    httpd = HTTPServer(server_address, RequestHandler)
    logger.info(f"Starting Mock Audio Server on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
