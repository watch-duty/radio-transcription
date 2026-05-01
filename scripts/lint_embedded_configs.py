#!/usr/bin/env python3
"""Lint embedded configs (nginx, etc.) in cloud-init .tftpl templates.

Walks terraform/ for .tftpl files, extracts embedded sub-configs from
cloud-init `write_files` blocks, runs each through its native validator
(currently nginx -t). Catches the bug class where terraform plan succeeds
but the rendered config is rejected at runtime — e.g., nginx `http_5xx`
that bit us on 2026-05-01 and crash-looped the autohealing aggregator
through 4 instance-recreate cycles before the host's iptables INPUT-DROP
diagnostic finally led back to the broken nginx parse.

Generic by design: any new nginx config we add to any .tftpl file gets
validated for free, no per-config rules. systemd / shellcheck / yamllint
extensions are easy to bolt on (one new `lint_*` function + a content-
detection heuristic).

CI usage:
  python3 scripts/lint_embedded_configs.py --terraform-dir terraform/

Local re-run against a single file:
  python3 scripts/lint_embedded_configs.py --terraform-dir terraform/modules/container_mig/
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# HCL template-var substitutions. We replace ${var} interpolations with safe
# placeholders so the extracted config can be parsed by its native validator.
# This is intentionally conservative: we don't try to evaluate %{if}/%{endif}
# branches — the rule is "every line in every branch must be valid syntax",
# regardless of whether the conditional happens to be true at apply time.
_PLACEHOLDER_SUBS: dict[str, str] = {
    r"\$\{service_name\}": "lint-service",
    r"\$\{container_image\}": "us-central1-docker.pkg.dev/lint-project/lint-repo/lint:lint",
    r"\$\{registry_host\}": "us-central1-docker.pkg.dev",
    r"\$\{env_file_content\}": "    LINT_PLACEHOLDER=true",
}

# Strip HCL conditional directives (%{ if ... } ... %{ endif }) without
# evaluating them. Keeps the body of every branch so we lint everything,
# including the branch the conditional would have skipped.
_HCL_CONDITIONAL_RE = re.compile(r"%\{\s*(?:if\s+[^}]*|else|endif)\s*\}")


def render_template(content: str) -> str:
    """Best-effort HCL .tftpl render for static lint purposes."""
    for pattern, replacement in _PLACEHOLDER_SUBS.items():
        content = re.sub(pattern, replacement, content)
    return _HCL_CONDITIONAL_RE.sub("", content)


# Matches a cloud-init `write_files` entry: `- path: <p>` ... `content: |`
# followed by an indented block. The block runs through any number of
# indented OR blank lines, ending at the next non-indented non-blank line
# (typically the next `- path:` entry or the start of the next top-level key).
_WRITE_FILE_RE = re.compile(
    r"^- path:\s*([^\n]+?)\s*\n"
    r"(?:[ \t]+[^\n]*\n)*?"  # intermediate keys (permissions, owner, etc.)
    r"[ \t]+content:[ \t]*\|\s*\n"
    r"((?:[ \t]+[^\n]*\n|\n)+)",  # indented OR blank lines, until next non-indented
    re.MULTILINE,
)


def _dedent_yaml_block(raw: str) -> str:
    """Strip common leading whitespace from a YAML `|` block."""
    lines = raw.split("\n")
    # Drop trailing empty lines so they don't skew the indent calculation
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return ""
    indents = [len(line) - len(line.lstrip()) for line in lines if line.strip()]
    indent = min(indents) if indents else 0
    return "\n".join(line[indent:] if len(line) >= indent else line for line in lines) + "\n"


def extract_write_files(rendered: str) -> list[tuple[str, str]]:
    """Return [(path, content), ...] for each write_files entry."""
    out: list[tuple[str, str]] = []
    for match in _WRITE_FILE_RE.finditer(rendered):
        path = match.group(1).strip().strip('"').strip("'")
        content = _dedent_yaml_block(match.group(2))
        out.append((path, content))
    return out


def looks_like_nginx(content: str) -> bool:
    """Heuristic: extracted file is nginx config if it has nginx-specific blocks."""
    return bool(re.search(r"^\s*(server|upstream|location|http)\s*\{", content, re.MULTILINE))


def lint_nginx(content: str) -> tuple[bool, str]:
    """Run `nginx -t` against `content`. Returns (passed, output)."""
    with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as f:
        f.write(content)
        path = f.name
    try:
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{path}:/etc/nginx/conf.d/default.conf:ro",
                "mirror.gcr.io/library/nginx:1.27-alpine",
                "nginx", "-t",
            ],
            capture_output=True, text=True, timeout=120,
            check=False,  # We inspect returncode ourselves below
        )
        output = (result.stderr or "") + (result.stdout or "")
        return result.returncode == 0, output
    finally:
        Path(path).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--terraform-dir",
        default="terraform/",
        help="Root directory to walk for .tftpl files (default: terraform/)",
    )
    args = parser.parse_args()

    root = Path(args.terraform_dir)
    if not root.exists():
        print(f"error: --terraform-dir {root} does not exist", file=sys.stderr)
        return 2

    tftpl_files = sorted(root.rglob("*.tftpl"))
    if not tftpl_files:
        print(f"No .tftpl files under {root} — nothing to lint.")
        return 0

    print(f"Scanning {len(tftpl_files)} .tftpl file(s) under {root}/")

    failures: list[tuple[Path, str, str]] = []
    checked = 0

    for tftpl_path in tftpl_files:
        rendered = render_template(tftpl_path.read_text())
        for embedded_path, content in extract_write_files(rendered):
            if not looks_like_nginx(content):
                continue
            checked += 1
            passed, output = lint_nginx(content)
            label = f"{tftpl_path}::{embedded_path}"
            if passed:
                print(f"  ok      {label}")
            else:
                print(f"  FAILED  {label}")
                # Emit GitHub Actions error annotation that points to the
                # source .tftpl file, not the temp file.
                print(f"::error file={tftpl_path}::Embedded nginx config "
                      f"'{embedded_path}' failed nginx -t:\n{output}")
                failures.append((tftpl_path, embedded_path, output))

    print(f"\nChecked {checked} embedded nginx config(s); "
          f"{len(failures)} failed.")

    if failures:
        print("\nLint failed. Each failure above includes the nginx -t output. "
              "Common causes: invalid directive value (e.g., http_5xx — nginx "
              "has no wildcard for status families, list individual codes); "
              "missing `;` after a directive; mismatched braces.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
