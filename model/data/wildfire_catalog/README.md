# Wildfire Radio Feed Catalog

A tool that discovers US radio scanner feeds relevant to wildfire from four sources (Broadcastify Feeds, Broadcastify Calls, OpenMHZ, Echo), scores them by wildfire relevance, and produces a priority-ranked catalog.

- **The tool** — all Python files at the top level (`run_catalog.py`, `scoring.py`, `geo_risk.py`, …) and in `sources/`. Run with `python run_catalog.py`.
- **The output** — `output/wildfire_feed_catalog.csv` (full) + `output/wildfire_feed_catalog_admin_review.csv` (Tier 1+2 only).
- **The findings** — see [`FINDINGS.md`](./FINDINGS.md) for the leadership brief covering ecosystem ceiling, OpenMHZ resource model, per-source duty cycles, GCS/AlloyDB cost deltas, and 10K-plan blockers.
