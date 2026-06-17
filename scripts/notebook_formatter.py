#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from typing import Any


def process_notebook(path: Path, *, write: bool = False) -> tuple[bool, bool]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading JSON from {path}: {e}\n")
        return False, False

    modified = False
    cells: Any = data.get("cells", [])
    if not isinstance(cells, list):
        return False, False

    for cell in cells:
        if not isinstance(cell, dict):
            continue
        cell_type = cell.get("cell_type")
        if cell_type == "markdown" and "execution_count" in cell:
            del cell["execution_count"]
            modified = True
        elif cell_type == "code" and "outputs" not in cell:
            cell["outputs"] = []
            modified = True

    if not (modified and write):
        return modified, True

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.write("\n")  # trailing newline
    except Exception as e:
        sys.stderr.write(f"Error writing {path}: {e}\n")
        return True, False

    return True, True


def report_results(modified_files: list[str], *, write_mode: bool) -> None:
    if write_mode:
        if modified_files:
            sys.stdout.write("Formatted the following notebooks:\n")
            for f in modified_files:
                sys.stdout.write(f"  {f}\n")
        else:
            sys.stdout.write("All notebooks are already well-formatted.\n")
    elif modified_files:
        sys.stdout.write(
            "The following notebooks need formatting or schema fixes:\n"
        )
        for f in modified_files:
            sys.stdout.write(f"  {f}\n")
        sys.exit(1)
    else:
        sys.stdout.write("All notebooks have valid schemas and formatting.\n")


def main() -> None:
    args = sys.argv[1:]
    write_mode = "--write" in args
    if write_mode:
        args.remove("--write")

    search_dir = Path("model/colabs")
    files = list(search_dir.rglob("*.ipynb"))

    if not files:
        sys.stdout.write("No notebooks found under model/colabs.\n")
        sys.exit(0)

    modified_files = []
    errors = False

    for f in files:
        is_modified, ok = process_notebook(f, write=write_mode)
        if not ok:
            errors = True
        if is_modified:
            modified_files.append(str(f))

    report_results(modified_files, write_mode=write_mode)

    if errors:
        sys.exit(2)


if __name__ == "__main__":
    main()
