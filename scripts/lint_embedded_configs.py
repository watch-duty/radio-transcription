#!/usr/bin/env python3
r"""Lint embedded configs (nginx, etc.) in cloud-init .tftpl templates.

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

Robust by design:
  * write_files extraction uses yaml.safe_load (not regex) so key
    ordering, quoting style, and comment placement don't matter.
  * Unknown ${var} references substitute to a safe placeholder so new
    terraform vars don't silently break the lint. ESCAPED $${...} (an
    nginx variable, not a terraform var) is left intact via negative
    lookbehind.
  * HCL `if/else/endif` AND `for/endfor` directives, including the
    whitespace-stripping `~` variants, are removed before parsing.
  * Mount path for nginx -t auto-detected: full nginx.conf (top-level
    `events {` / `http {`) vs conf.d snippet (just `server {` /
    `upstream {`).
  * GitHub Actions `::error` annotations URL-encode newlines (%0A) so
    the full nginx -t output renders inline in the PR diff annotation,
    not just the first line.

Known limitations (intentional trade-offs):
  * HCL `if/else` branches are NOT evaluated — both bodies are kept
    and linted as one. If an `if/else` defines mutually-exclusive
    nginx directives (e.g., different `ssl_certificate` per env),
    nginx -t will see both and reject as duplicate. Workarounds:
    define each variant in its own .tftpl, or accept the lint
    requires the union of all branches to be valid.
  * `looks_like_nginx()` requires a block declaration (`server {`,
    `upstream {`, etc.) to identify nginx content. Pure-directive
    snippets meant to be `include`'d elsewhere (e.g., a file with
    only `client_max_body_size 50m;`) are silently SKIPPED — they
    don't match the heuristic. If you ship such snippets, either add
    a wrapping comment that triggers detection, or extend the
    heuristic with a directive-only pattern.
  * Unknown ${var} substitutes to the STRING `lint_placeholder`,
    which is valid for many nginx contexts (paths, hostnames,
    identifiers) but FALSE-POSITIVE-fails for numeric/size/time
    contexts: `keepalive_timeout ${tf_var};` renders to
    `keepalive_timeout lint_placeholder;` and nginx -t rejects with
    `invalid value`. Workaround: add the var to
    `_KNOWN_PLACEHOLDER_SUBS` with a typed default (e.g., `"65"` for
    a timeout var). The catch-all is intentionally string-typed
    because nginx context can't be inferred from regex alone.
  * `_KNOWN_PLACEHOLDER_SUBS` regexes are STRICT: `\$\{service_name\}`
    matches only the bare form, not scoped variants like
    `${var.service_name}` or whitespace-padded `${ service_name }`.
    The catch-all fallback substitutes scoped/spaced variants too,
    so they don't break the lint — they just hit the string-typed
    placeholder limitation above. If your team uses scoped variants
    routinely and bumps into the typing issue, loosen the regex
    (e.g., `\$\{(?:var\.)?service_name\}`) or add the scoped form
    as its own entry.

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

import yaml

# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

# Known terraform vars get type-appropriate placeholders so the rendered
# YAML is plausible (e.g., a registry hostname looks like a hostname).
# Anything not listed here falls through to _UNKNOWN_VAR_PLACEHOLDER.
_KNOWN_PLACEHOLDER_SUBS: dict[str, str] = {
    r"\$\{service_name\}": "lint-service",
    r"\$\{container_image\}": "us-central1-docker.pkg.dev/lint-project/lint-repo/lint:lint",
    r"\$\{registry_host\}": "us-central1-docker.pkg.dev",
    r"\$\{env_file_content\}": "    LINT_PLACEHOLDER=true",
}

# Fallback for any ${...} reference not in _KNOWN_PLACEHOLDER_SUBS. New
# terraform vars get free coverage; the lint never silently leaves raw
# ${var} syntax for nginx (which would interpret it as an nginx variable
# and emit "[emerg] unknown variable").
#
# Negative lookbehind `(?<!\$)` skips ESCAPED terraform syntax like
# `$${nginx_var}` — in HCL templates, `$$` is the escape for a literal
# `$` and means "this is an nginx variable, not a terraform var". After
# templatefile() renders, `$${nginx_var}` becomes `${nginx_var}` for
# nginx to interpolate at request time. We must NOT touch those.
_UNKNOWN_VAR_RE = re.compile(r"(?<!\$)\$\{[^}]+\}")
_UNKNOWN_VAR_PLACEHOLDER = "lint_placeholder"

# Strip HCL directives (if/else/endif AND for/endfor, with optional
# whitespace-stripping `~` on either side). Keeps the body of every
# branch / iteration so we lint everything, regardless of which branch
# would have been chosen at apply time.
_HCL_DIRECTIVE_RE = re.compile(
    r"%\{~?\s*"
    r"(?:if\s+[^}]*|else|endif|for\s+[^}]*|endfor)"
    r"\s*~?\}",
)


def render_template(content: str) -> str:
    """Best-effort HCL .tftpl render for static lint purposes."""
    for pattern, replacement in _KNOWN_PLACEHOLDER_SUBS.items():
        content = re.sub(pattern, replacement, content)
    # Catch-all for unknown ${var} references (new vars get safe coverage)
    content = _UNKNOWN_VAR_RE.sub(_UNKNOWN_VAR_PLACEHOLDER, content)
    return _HCL_DIRECTIVE_RE.sub("", content)


# ---------------------------------------------------------------------------
# write_files extraction (yaml-based, robust to key ordering)
# ---------------------------------------------------------------------------


def extract_write_files(
    rendered: str,
    source_path: Path | None = None,
) -> list[tuple[str, str]]:
    """Parse rendered cloud-init YAML; return [(path, content), ...] for each
    write_files entry. Robust to key ordering — uses yaml.safe_load instead
    of a regex that assumed `path:` precedes `content:`.

    cloud-init files start with `#cloud-config` (a comment), so they're
    valid YAML. If the rendered output isn't parseable, returns [] AND
    emits a warning to stderr (with a GitHub Actions ::warning annotation
    on the source file when known) — silent skips would mask broken
    templates and give false confidence that the lint passed.
    """
    try:
        parsed = yaml.safe_load(rendered)
    except yaml.YAMLError as e:
        # Only warn for files that look like cloud-config (avoid noise on
        # other .tftpl shapes — e.g., a raw nginx.conf.tftpl with no YAML
        # wrapper, or a template that's only valid after templatefile()).
        if "#cloud-config" in rendered:
            location = f" in {source_path}" if source_path else ""
            print(
                f"warning: cloud-config YAML parse failed{location}; "
                f"embedded-config lint skipped for this file: {e}",
                file=sys.stderr,
            )
            if source_path is not None:
                # Surface in PR diff annotation so reviewers see it
                print(
                    f"::warning file={source_path}::cloud-config YAML parse "
                    f"failed; embedded-config lint skipped. Fix the YAML "
                    f"structure (or HCL render artifacts) and re-push.",
                )
        return []
    if not isinstance(parsed, dict):
        return []
    write_files = parsed.get("write_files") or []
    if not isinstance(write_files, list):
        return []
    out: list[tuple[str, str]] = []
    for entry in write_files:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        content = entry.get("content")
        if isinstance(path, str) and isinstance(content, str):
            out.append((path, content))
    return out


# ---------------------------------------------------------------------------
# nginx detection + validation
# ---------------------------------------------------------------------------

# Snippet-level blocks (go inside http{}): server, upstream, location.
_NGINX_SNIPPET_RE = re.compile(
    r"^\s*(?:server|upstream|location)\s*\{", re.MULTILINE
)
# Top-level blocks (must be at file root): events, http, stream, mail.
_NGINX_TOPLEVEL_RE = re.compile(
    r"^\s*(?:events|http|stream|mail)\s*\{", re.MULTILINE
)


def looks_like_nginx(content: str) -> bool:
    """Heuristic: extracted file is nginx config if it has nginx-specific blocks."""
    return bool(
        _NGINX_SNIPPET_RE.search(content) or _NGINX_TOPLEVEL_RE.search(content)
    )


def is_full_nginx_conf(content: str) -> bool:
    """A 'full' nginx.conf has top-level events{} or http{}; a 'snippet' has
    only server/upstream/location blocks (which must be inside http{} at
    parse time, so it must be mounted into conf.d/, not as nginx.conf).
    """
    return bool(_NGINX_TOPLEVEL_RE.search(content))


def lint_nginx(content: str) -> tuple[bool, str]:
    """Run `nginx -t` against `content`. Returns (passed, output).

    Mount path is auto-selected:
      - Full nginx.conf (events{}, http{}) → /etc/nginx/nginx.conf (replaces
        the image's default; otherwise the existing http{} would conflict
        with our http{} → '"http" directive is not allowed here' error)
      - Snippet (server{}, upstream{}, location{}) → /etc/nginx/conf.d/default.conf
        (gets included inside the image's existing http{} block)
    """
    full_conf = is_full_nginx_conf(content)
    mount_path = (
        "/etc/nginx/nginx.conf"
        if full_conf
        else "/etc/nginx/conf.d/default.conf"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as f:
        f.write(content)
        tmp_path = f.name
    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{tmp_path}:{mount_path}:ro",
                "mirror.gcr.io/library/nginx:1.27-alpine",
                "nginx",
                "-t",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,  # We inspect returncode ourselves below
        )
        output = (result.stderr or "") + (result.stdout or "")
        return result.returncode == 0, output
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _process_tftpl(tftpl_path: Path) -> list[tuple[Path, str, str]]:
    """Returns list of (tftpl_path, embedded_path, error_output) for each
    embedded nginx config in the file that fails validation. Empty list on
    success.
    """
    rendered = render_template(tftpl_path.read_text())
    extracted: list[tuple[str, str]] = extract_write_files(
        rendered, source_path=tftpl_path
    )
    failures: list[tuple[Path, str, str]] = []
    for embedded_path, content in extracted:
        if not looks_like_nginx(content):
            continue
        passed, output = lint_nginx(content)
        label = f"{tftpl_path}::{embedded_path}"
        if passed:
            print(f"  ok      {label}")
        else:
            print(f"  FAILED  {label}")
            # Print human-readable output to logs first (with real newlines)
            print(output)
            # Then emit the GitHub Actions annotation. Workflow commands
            # don't tolerate literal newlines — anything past the first \n
            # falls out of the ::error and renders as plain log output, so
            # the inline PR-diff annotation only shows the first line.
            # URL-encoding \n as %0A keeps the multi-line nginx -t output
            # inside the annotation block.
            encoded_output = output.replace("\n", "%0A")
            print(
                f"::error file={tftpl_path}::Embedded nginx config "
                f"'{embedded_path}' failed nginx -t:%0A{encoded_output}",
            )
            failures.append((tftpl_path, embedded_path, output))
    return failures


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

    all_failures: list[tuple[Path, str, str]] = []
    for tftpl_path in tftpl_files:
        all_failures.extend(_process_tftpl(tftpl_path))

    print(f"\n{len(all_failures)} embedded nginx config(s) failed validation.")

    if all_failures:
        print(
            "\nLint failed. Each failure above includes the nginx -t output. "
            "Common causes: invalid directive value (e.g., http_5xx — nginx "
            "has no wildcard for status families, list individual codes); "
            "missing `;` after a directive; mismatched braces.",
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
