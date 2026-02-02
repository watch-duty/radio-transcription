#!/usr/bin/env python3
"""
Extract filename_template values from file-type outputs in rtl_airband configuration files.

This script parses rtl_airband config files and extracts all filename_template
values from outputs where type = "file" within the devices section.
"""

import re
import sys
from pathlib import Path


def extract_filename_templates(config_path) -> list[str]:
    """
    Extract filename_template values from file-type outputs in devices section.

    Args:
        config_path: Path to the rtl_airband configuration file

    Returns:
        List of filename_template values

    """
    with open(config_path) as f:
        content = f.read()

    # Find the devices section - need to match balanced parentheses
    devices_start = content.find("devices:")
    if devices_start == -1:
        print("Warning: No devices section found", file=sys.stderr)
        return []

    # Find the opening parenthesis after "devices:"
    paren_start = content.find("(", devices_start)
    if paren_start == -1:
        print("Warning: No opening parenthesis found after devices:", file=sys.stderr)
        return []

    # Find matching closing parenthesis
    paren_level = 0
    paren_end = -1
    for i in range(paren_start, len(content)):
        if content[i] == "(":
            paren_level += 1
        elif content[i] == ")":
            paren_level -= 1
            if paren_level == 0:
                paren_end = i
                break

    if paren_end == -1:
        print("Warning: No matching closing parenthesis found for devices section", file=sys.stderr)
        return []

    devices_content = content[paren_start+1:paren_end]

    filename_templates = []

    # Find all file-type outputs with filename_template
    # Look for type = "file" followed by filename_template = "..."
    # These are within output blocks which are within outputs: ( ... )

    # Simple approach: find all blocks with type = "file" and extract filename_template
    lines = devices_content.split("\n")
    in_file_output = False

    for i, line in enumerate(lines):
        # Check if we're entering a file output block
        if 'type = "file"' in line:
            in_file_output = True
        # If we're in a file output block, look for filename_template
        elif in_file_output and "filename_template" in line:
            template_match = re.search(r'filename_template\s*=\s*"([^"]+)"', line)
            if template_match:
                filename_templates.append(template_match.group(1))
            in_file_output = False
        # Exit file output block if we hit a closing brace
        elif in_file_output and "}" in line:
            in_file_output = False

    return filename_templates


def main():
    if len(sys.argv) != 2:
        print("Usage: python make_mono_echo_feeds_csv.py <config_file>", file=sys.stderr)
        sys.exit(1)

    config_path = Path(sys.argv[1])

    if not config_path.exists():
        print(f"Error: File not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    echo_name = config_path.stem.replace("rtl_airband-", "")
    templates = extract_filename_templates(config_path)
    templates = list(set(templates))  # unique

    # Print each template on a new line
    for template in templates:
        if not template.endswith("_tones"):
            print(f'"{echo_name}","{template}"')


if __name__ == "__main__":
    main()
