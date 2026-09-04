#!/usr/bin/env python3
"""Fails if an ingestion migration creates an index a later migration drops.

Migrations replay on every deploy, so any index that is both created and
dropped, in the same file or across files, is rebuilt on each run. Remove the
CREATE (keeping a DROP as the safety net), or edit the original CREATE to
redefine an index. Stdlib-only so CI can run it bare.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_SQL_DIR = (
    Path(__file__).resolve().parents[1]
    / "terraform"
    / "modules"
    / "alloydb"
    / "sql"
    / "ingestion"
)

_LINE_COMMENT = re.compile(r"--[^\n]*")
# Stripped before matching: 037's HINT text spells out a DROP INDEX command.
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")

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


def _active_sql(content: str) -> str:
    """Returns content with line comments and string literals removed."""
    return _STRING_LITERAL.sub("''", _LINE_COMMENT.sub("", content))


def main() -> int:
    creates: dict[str, str] = {}  # index name -> file that creates it
    errors: list[str] = []

    for sql_file in sorted(_SQL_DIR.glob("*.sql"), key=lambda f: f.name):
        sql = _active_sql(sql_file.read_text())

        dropped_here = {
            _normalize(name)
            for match in _DROP_INDEX.finditer(sql)
            for name in match.group("names").split(",")
        }
        created_here = {
            _normalize(match.group("name"))
            for match in _CREATE_INDEX.finditer(sql)
        }

        for name in created_here:
            creates.setdefault(name, sql_file.name)

        for name in sorted(dropped_here):
            if name in created_here:
                errors.append(
                    f"{sql_file.name} drops and creates index '{name}': it is "
                    f"rebuilt on every replay. Edit the index's original "
                    f"CREATE instead of drop-and-recreate."
                )
            elif name in creates:
                errors.append(
                    f"{sql_file.name} drops index '{name}' created by "
                    f"{creates[name]}: it is rebuilt and dropped on every "
                    f"replay. Remove the CREATE from {creates[name]} and keep "
                    f"this DROP as the safety net."
                )

    if errors:
        sys.stderr.write("Migration churn check failed:\n\n")
        for error in errors:
            sys.stderr.write(f"  - {error}\n")
        return 1

    sys.stdout.write("No index create/drop churn found in migrations.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
