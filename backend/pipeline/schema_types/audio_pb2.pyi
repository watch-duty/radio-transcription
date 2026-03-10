from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Audio(_message.Message):
    __slots__ = ("gcs_file_path",)
    GCS_FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    gcs_file_path: str
    def __init__(self, gcs_file_path: _Optional[str] = ...) -> None: ...
