#!/usr/bin/env python3
"""Fails if an ingestion migration creates an index another migration drops.

Migrations replay on every deploy, so any index that is both created and
dropped, in the same file or across files in either order, is rebuilt on each
run. Remove whichever half is obsolete, or edit the original CREATE to
redefine an index. Stdlib-only so CI can run it bare.

This check exists only because migrations replay. Once they move to a
run-once ledger, it can be deleted.
"""

from __future__ import annotations

import itertools
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

_SQL_DIR = (
    Path(__file__).resolve().parents[1]
    / "terraform"
    / "modules"
    / "alloydb"
    / "sql"
    / "ingestion"
)

# One pass that consumes whichever comes first, a string literal or a line
# comment, so a "--" inside a literal cannot swallow the literal's closing
# quote. Stripped before matching: 037's HINT text spells out a DROP INDEX.
_LITERAL_OR_COMMENT = re.compile(r"'(?:[^']|'')*'|--[^\n]*")

_CREATE_INDEX = re.compile(
    r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[\w\".]+)",
    re.IGNORECASE,
)
_DROP_INDEX = re.compile(
    r"\bDROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?"
    r"(?P<names>[\w\".]+(?:\s*,\s*[\w\".]+)*)",
    re.IGNORECASE,
)


def _normalize(name: str) -> str:
    """Strips schema qualification and quoting: public."Foo" -> foo."""
    return name.rsplit(".", maxsplit=1)[-1].replace('"', "").lower()


def _strip(match: re.Match[str]) -> str:
    return "''" if match.group().startswith("'") else ""


def _active_sql(content: str) -> str:
    """Returns content with line comments and string literals removed."""
    return _LITERAL_OR_COMMENT.sub(_strip, content)


def _created_names(sql: str) -> Iterator[str]:
    """Yields normalized names of indexes the SQL creates."""
    for match in _CREATE_INDEX.finditer(sql):
        yield _normalize(match.group("name"))


def _dropped_names(sql: str) -> Iterator[str]:
    """Yields normalized names of indexes the SQL drops."""
    for match in _DROP_INDEX.finditer(sql):
        for raw in match.group("names").split(","):
            yield _normalize(raw)


def _churn_message(name: str, created_in: str, dropped_in: str) -> str:
    if created_in == dropped_in:
        return (
            f"{created_in} drops and creates index '{name}': it is rebuilt on "
            f"every replay. Edit the index's original CREATE instead of "
            f"drop-and-recreate."
        )
    return (
        f"{created_in} creates index '{name}' and {dropped_in} drops it, so it "
        f"is rebuilt and dropped on every replay. Remove whichever is "
        f"obsolete. Keep the DROP only if the index should be absent."
    )


def _churn_messages(
    name: str, created_in: set[str], dropped_in: set[str]
) -> list[str]:
    pairs = itertools.product(sorted(created_in), sorted(dropped_in))
    return [_churn_message(name, c, d) for c, d in pairs]


def find_churn(sql_dir: Path) -> list[str]:
    """Returns one message per create/drop pair found across sql_dir."""
    creates: dict[str, set[str]] = {}  # index name -> files that create it
    drops: dict[str, set[str]] = {}  # index name -> files that drop it

    for sql_file in sorted(sql_dir.glob("*.sql"), key=lambda f: f.name):
        sql = _active_sql(sql_file.read_text())
        for name in _created_names(sql):
            creates.setdefault(name, set()).add(sql_file.name)
        for name in _dropped_names(sql):
            drops.setdefault(name, set()).add(sql_file.name)

    errors: list[str] = []
    for name in sorted(creates.keys() & drops.keys()):
        errors.extend(_churn_messages(name, creates[name], drops[name]))
    return errors


def main() -> int:
    errors = find_churn(_SQL_DIR)
    if errors:
        sys.stderr.write("Migration churn check failed:\n\n")
        for error in errors:
            sys.stderr.write(f"  - {error}\n")
        return 1

    sys.stdout.write("No index create/drop churn found in migrations.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
