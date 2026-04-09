# Raster Image Repository

Minimal agent-friendly scaffold for incrementally inventorying a messy raster estate Incrementally convert a large, disorganized legacy raster archive into a **consistent, cloud-friendly, queryable dataset**.

Status: **Alpha** - everything subject to change. Zero guarantees on usefulness or longevity.

## What this does

The first script walks one or more directories, finds likely raster files, runs `gdalinfo -json` against each, and stores a lightweight inventory in SQLite.

This is intentionally the **inventory phase only**:

- no conversion
- no reprojection
- no STAC output yet
- no destructive changes

That keeps it safe to run in short bursts while you build coverage over time.

## Project layout

```text
raster-repo/
  AGENTS.md
  README.md
  src/
    raster_inventory/
      __init__.py
      inventory.py
      viewer.py
    raster_repo/
      __init__.py
```

## Prerequisites

- Python 3.12+
- GDAL installed (Python bindings or CLI)
  - Python: activate venv and `uv run scripts/install-py-gdal.py`
  - CLI: ensure `gdalinfo` is on `PATH`
  - The CLI falls back to the Python bindings when `gdalinfo` is missing.


## Quick start

```bash
# Create a virtual environment and install Python dependencies:
uv sync
.venv/Scripts/activate

# install gdal binary python bindings
uv run scripts/install-py-gdal.py

# Install the main cli into the virtual environment:
uv tool install -e .

# quick CLI check
raster-repo --help
raster-repo view --help

# python -m entrypoint
uv run python -m raster_inventory --help
```
Run an inventory over one or more roots:

```bash
raster-repo \
  --db inventory.sqlite \
  /data/raster_root_1 /data/raster_root_2
```

Show summary stats:

```bash
raster-repo \
  --db inventory.sqlite \
  --summary
```

Browse existing inventory rows without rescanning:

```bash
raster-repo view \
  --db inventory.sqlite \
  --limit 30 \
  --order mtime
```

Rescan only files that are new or changed:

```bash
raster-repo \
  --db inventory.sqlite \
  --skip-unchanged \
  /data/raster_root

## Testing

Run the pytest suite before pushing changes:

```bash
uv run pytest -v
```
```

## Suggested next steps

After the inventory is working, the next increments are:

 1. add a triage table for issues like `not_cog`, `float_classes`, `missing_crs`
 2. add a processing queue table
 3. emit minimal STAC items for inventoried rasters
 4. add COG conversion profiles

## Notes

The script uses a conservative extension filter to avoid calling `gdalinfo` on everything. Expand `DEFAULT_EXTENSIONS` as needed for your estate.

----

# Overview

**Goal:** incrementally convert a large, disorganized legacy raster archive into a **consistent, cloud-friendly, queryable dataset**.

---

## Target state

* All rasters in **COG (or equivalent cloud-optimized format)**
* Each asset described in a **STAC catalog**
* Data is:

  * efficiently readable (range requests, tiling)
  * consistently encoded (dtype, compression, overviews)
  * discoverable (spatial + metadata queries)

---

## Core approach

1. **Inventory first (no mutation)**

   * Build a SQLite catalog of all rasters and their properties

2. **Classify issues**

   * Identify problems (non-COG, bad dtype, missing CRS, etc.)

3. **Normalize incrementally**

   * Convert selected files → standardized COG profiles

4. **Publish metadata alongside**

   * Generate STAC items as files are processed

5. **Track everything**

   * Ensure work is resumable, idempotent, and queryable

---

## Operating model

* Work in **small batches** (10–50 files/session)
* Prioritize:

  * easy wins
  * high-value datasets
* Continuously improve rules as edge cases appear

---

## End result

A system where:

* raw legacy data becomes **managed assets**
* processing is **repeatable and automatable**
* the archive evolves from a “pile of files” into a **structured data platform**
