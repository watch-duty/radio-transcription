import base64
import os
import sys
import urllib.parse

import requests
from fn_types import FNFile, FNListDirResponse

FN_API_LIST_DIR_URL = (
    "https://player.textmefires.info/audioplay/audio_files_new.cgi?dir={dir}"
)
FN_AUDIO_FILE_BASE_URL = "https://player.textmefires.info/audioplay/audio/"

FN_AUTH_USERNAME = "watchduty"
FN_AUTH_PASSWORD = os.environ.get("FN_AUTH_PASSWORD")


def get_auth_headers() -> dict[str, str]:
    if not FN_AUTH_PASSWORD:
        print(
            "ERROR: FN_AUTH_PASSWORD environment variable is not set.", file=sys.stderr
        )
        sys.exit(1)
    unencoded_str = f"{FN_AUTH_USERNAME}:{FN_AUTH_PASSWORD}"
    b64_str = base64.b64encode(unencoded_str.encode()).decode()
    return {"Authorization": f"Basic {b64_str}"}


def list_fn_dir(dir: str) -> FNListDirResponse:
    """
    Returns a tuple of a list of sub-dirs, and a list of files. All are full paths.
    """
    # print(f"DEBUG: ls {dir}", file=sys.stderr)
    request_url = FN_API_LIST_DIR_URL.format(dir=dir)
    response = requests.get(request_url, headers=get_auth_headers())
    response.raise_for_status()

    # response schema:
    # {
    #   "dir": "<input dir>",
    #   "dir_inode": ...,
    #   "dir_list": [
    #     {
    #       "file": "<name>",
    #       "filesize": "<DIR>",
    #       "filetype": "dir",  # or "back_dir"
    #       "inode": "...",
    #       "path": "<input dir>",
    #     },
    #     ...
    #   ],
    #   "file_list": [
    #     {
    #       "file": "<name>",
    #       "filesize": "...",
    #       "filetype": "file",
    #       "inode": "...",
    #       "path": "<input dir>",
    #     },
    #     ...
    #   ],
    # }

    data = response.json()
    # grab only the sub-directories
    dirs_raw = [d for d in data["dir_list"] if d["filetype"] == "dir"]

    # and the files
    files_raw = [f for f in data["file_list"] if f["filetype"] == "file"]

    dirs = [d["path"] + "/" + d["file"] for d in dirs_raw]
    files = [
        FNFile(path=d["path"] + "/" + d["file"], size_bytes=int(d["filesize"]))
        for d in files_raw
    ]
    return FNListDirResponse(
        dirs=dirs,
        files=files,
    )

def make_fn_audio_url(path: str) -> str:
    path_encoded = urllib.parse.quote(path)
    return f"{FN_AUDIO_FILE_BASE_URL}/{path_encoded}"
