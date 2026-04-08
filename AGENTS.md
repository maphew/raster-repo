# Project guidance for Agents

## Objective

Help incrementally normalize a large, messy raster estate toward:

- COG or COG-like storage
- STAC exposure
- repeatable, low-risk daily progress

## Current phase

Phase 1 only: inventory and metadata capture into SQLite.

## Constraints

- Prefer Python and SQLite.
- Prefer GDAL (Python bindings or CLI) over ArcPy.
- Do not overwrite source rasters.
- Keep changes small and reviewable.
- Favor idempotent scripts and append-only metadata collection.
- Avoid premature architecture.

## When editing code

- Keep dependencies minimal.
- Add docstrings where they clarify behavior.
- Keep CLI flags explicit rather than clever.
- Prefer standard library unless a small dependency materially improves clarity.
- Preserve backward-compatible database migrations when possible.

## Near-term roadmap

1. inventory rasters into SQLite
2. add issue/triage detection
3. create processing queue
4. add conversion/validation pipeline
5. generate STAC items and collections

## Validation

For code changes, prefer running:

```bash
uv run python -m raster_inventory.inventory --help
```

If a test suite is added later, run it before finishing.
