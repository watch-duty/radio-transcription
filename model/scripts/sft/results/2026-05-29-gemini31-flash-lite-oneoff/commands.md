# Prepared tuning commands

Run these only when ready to submit paid Vertex tuning jobs.

```bash
cd model/scripts/sft
PYTHONPATH=../../colabs:. python pipeline.py tune --round-id 2026-05-29-wd-internal-a4-gemini31-flash-lite --base-model gemini-3.1-flash-lite --epochs 5 --adapter-size FOUR --lr-multiplier 1.0 --confirm
PYTHONPATH=../../colabs:. python pipeline.py tune --round-id 2026-05-29-wd-internal-a8-gemini31-flash-lite --base-model gemini-3.1-flash-lite --epochs 5 --adapter-size EIGHT --lr-multiplier 1.0 --confirm
PYTHONPATH=../../colabs:. python pipeline.py tune --round-id 2026-05-29-wd-atc-a4-gemini31-flash-lite --base-model gemini-3.1-flash-lite --epochs 5 --adapter-size FOUR --lr-multiplier 1.0 --confirm
PYTHONPATH=../../colabs:. python pipeline.py tune --round-id 2026-05-29-wd-atc-a8-gemini31-flash-lite --base-model gemini-3.1-flash-lite --epochs 5 --adapter-size EIGHT --lr-multiplier 1.0 --confirm
```
