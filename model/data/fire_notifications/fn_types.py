from dataclasses import dataclass


@dataclass
class FNFile:
    path: str
    size_bytes: int


@dataclass
class FNListDirResponse:
    dirs: list[str]

    files: list[FNFile]
