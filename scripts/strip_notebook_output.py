#!/usr/bin/env python3
"""Git clean filter that strips outputs and execution counts from Jupyter notebooks.

Reads a notebook from stdin and writes the cleaned version to stdout.
Uses indent=4 to match the existing notebook formatting in this repo.

Setup (local only):
    git config filter.strip-notebook-output.clean 'python3 scripts/strip_notebook_output.py'
    git config filter.strip-notebook-output.smudge cat

Requires .git/info/attributes to contain:
    *.ipynb filter=strip-notebook-output
"""

import json
import sys


def strip_notebook(notebook: dict) -> dict:
    """Strip outputs and execution counts from a notebook dict."""
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
    return notebook


def main() -> None:
    try:
        notebook = json.load(sys.stdin)
    except json.JSONDecodeError:
        # If it's not valid JSON, pass through unchanged
        sys.stdin.seek(0)
        sys.stdout.write(sys.stdin.read())
        return

    notebook = strip_notebook(notebook)
    json.dump(notebook, sys.stdout, indent=4, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
