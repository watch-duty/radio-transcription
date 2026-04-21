import os

paths = [
    "backend/pipeline/transcription/tests/test_audio_processor.py",
    "backend/pipeline/transcription/tests/test_transforms.py",
]

for path in paths:
    with open(path, "r") as f:
        lines = f.readlines()

    new_lines = []
    has_pytest = False
    for line in lines:
        if "import pytest" in line and "pytest.mark.skip" not in line:
            has_pytest = True
            continue  # skip this line
        new_lines.append(line)

    if has_pytest:
        # insert at top (after __future__ or at very top)
        new_lines.insert(0, "import pytest\n")

    with open(path, "w") as f:
        f.writelines(new_lines)
