from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from types import ModuleType

DEFAULT_EXTENSIONS = {
    ".tif",
    ".tiff",
    ".img",
    ".vrt",
    ".jp2",
    ".sid",
    ".ecw",
    ".bil",
    ".hdr",
    ".adf",
    ".grd",
    ".nc",
    ".hdf",
    ".hdf5",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    size_bytes INTEGER,
    mtime_utc TEXT,
    format TEXT,
    driver TEXT,
    crs TEXT,
    width INTEGER,
    height INTEGER,
    bands INTEGER,
    dtype TEXT,
    nodata TEXT,
    is_cog INTEGER,
    has_overviews INTEGER,
    raw_json TEXT,
    scan_status TEXT NOT NULL,
    error_message TEXT,
    scanned_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_files_scan_status ON files(scan_status);
CREATE INDEX IF NOT EXISTS idx_files_is_cog ON files(is_cog);
"""


@dataclass(slots=True)
class FileRecord:
    path: str
    size_bytes: int
    mtime_utc: str
    format: str | None
    driver: str | None
    crs: str | None
    width: int | None
    height: int | None
    bands: int | None
    dtype: str | None
    nodata: str | None
    is_cog: int | None
    has_overviews: int | None
    raw_json: str | None
    scan_status: str
    error_message: str | None
    scanned_at_utc: str


_GDAL_MODULE: ModuleType | None = None


def import_gdal() -> ModuleType:
    """Import and cache the GDAL Python module."""

    global _GDAL_MODULE
    if _GDAL_MODULE is None:
        from osgeo import gdal as module  # type: ignore[import-not-found]

        module.UseExceptions()
        _GDAL_MODULE = module
    return _GDAL_MODULE


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventory raster files into SQLite using gdalinfo -json."
    )
    parser.add_argument(
        "roots",
        nargs="*",
        type=Path,
        help="One or more directory roots to scan.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("inventory.sqlite"),
        help="SQLite database path. Default: inventory.sqlite",
    )
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=sorted(DEFAULT_EXTENSIONS),
        help="File extensions to include. Default: common raster extensions.",
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="Skip files whose size and mtime match an existing record.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print database summary and exit.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after scanning N candidate files.",
    )
    parser.add_argument(
        "--store-raw-json",
        action="store_true",
        help="Store full gdalinfo JSON in the database.",
    )
    return parser


def normalize_extensions(values: Iterable[str]) -> set[str]:
    normalized: set[str] = set()
    for value in values:
        value = value.strip().lower()
        if not value:
            continue
        if not value.startswith("."):
            value = f".{value}"
        normalized.add(value)
    return normalized


def ensure_gdalinfo() -> None:
    if shutil.which("gdalinfo") is not None:
        return
    try:
        import_gdal()
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "gdalinfo was not found on PATH and GDAL Python bindings are unavailable."
        ) from exc


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA_SQL)
    return conn


def iter_candidate_files(roots: Iterable[Path], extensions: set[str]) -> Iterable[Path]:
    for root in roots:
        if not root.exists():
            print(f"[warn] root does not exist: {root}", file=sys.stderr)
            continue
        if root.is_file():
            if root.suffix.lower() in extensions:
                yield root
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in extensions:
                yield path


def get_existing_signature(
    conn: sqlite3.Connection, path: str
) -> tuple[int, str] | None:
    row = conn.execute(
        "SELECT size_bytes, mtime_utc FROM files WHERE path = ?",
        (path,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def path_signature(path: Path) -> tuple[int, str]:
    stat = path.stat()
    mtime = (
        datetime.fromtimestamp(stat.st_mtime, tz=UTC).replace(microsecond=0).isoformat()
    )
    return stat.st_size, mtime


def run_gdalinfo(path: Path) -> dict:
    if shutil.which("gdalinfo") is not None:
        return run_gdalinfo_cli(path)
    return run_gdalinfo_python(path)


def run_gdalinfo_cli(path: Path) -> dict:
    result = subprocess.run(
        ["gdalinfo", "-json", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or "gdalinfo failed"
        raise RuntimeError(stderr)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from gdalinfo: {exc}") from exc


def run_gdalinfo_python(path: Path) -> dict:
    gdal = import_gdal()
    try:
        output = gdal.Info(str(path), format="json")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(str(exc)) from exc
    if not output:
        raise RuntimeError("gdal.Info returned no output")
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from gdal.Info: {exc}") from exc


def first_band_type(info: dict) -> str | None:
    bands = info.get("bands") or []
    if not bands:
        return None
    value = bands[0].get("type")
    return str(value) if value is not None else None


def first_band_nodata(info: dict) -> str | None:
    bands = info.get("bands") or []
    if not bands:
        return None
    band = bands[0]
    if "noDataValue" not in band:
        return None
    return str(band["noDataValue"])


def has_overviews(info: dict) -> int | None:
    bands = info.get("bands") or []
    if not bands:
        return None
    return 1 if any((band.get("overviews") or []) for band in bands) else 0


def is_cog(info: dict) -> int | None:
    driver_short = info.get("driverShortName")
    image_structure = info.get("metadata", {}).get("IMAGE_STRUCTURE", {})
    layout = image_structure.get("LAYOUT")
    if driver_short == "COG":
        return 1
    if str(layout).upper() == "COG":
        return 1
    return 0 if driver_short is not None else None


def extract_record(path: Path, info: dict, store_raw_json: bool) -> FileRecord:
    size_bytes, mtime_utc = path_signature(path)
    size = info.get("size") or [None, None]
    bands = info.get("bands") or []
    coordinate_system = info.get("coordinateSystem") or {}
    wkt = coordinate_system.get("wkt")
    driver_short = info.get("driverShortName")
    driver_long = info.get("driverLongName")
    return FileRecord(
        path=str(path),
        size_bytes=size_bytes,
        mtime_utc=mtime_utc,
        format=str(driver_short) if driver_short is not None else None,
        driver=str(driver_long) if driver_long is not None else None,
        crs=str(wkt) if wkt else None,
        width=size[0],
        height=size[1],
        bands=len(bands) if bands else 0,
        dtype=first_band_type(info),
        nodata=first_band_nodata(info),
        is_cog=is_cog(info),
        has_overviews=has_overviews(info),
        raw_json=json.dumps(info, separators=(",", ":")) if store_raw_json else None,
        scan_status="ok",
        error_message=None,
        scanned_at_utc=utc_now(),
    )


def error_record(path: Path, error: str) -> FileRecord:
    size_bytes, mtime_utc = path_signature(path)
    return FileRecord(
        path=str(path),
        size_bytes=size_bytes,
        mtime_utc=mtime_utc,
        format=None,
        driver=None,
        crs=None,
        width=None,
        height=None,
        bands=None,
        dtype=None,
        nodata=None,
        is_cog=None,
        has_overviews=None,
        raw_json=None,
        scan_status="error",
        error_message=error,
        scanned_at_utc=utc_now(),
    )


def upsert_record(conn: sqlite3.Connection, record: FileRecord) -> None:
    conn.execute(
        """
        INSERT INTO files (
            path, size_bytes, mtime_utc, format, driver, crs, width, height,
            bands, dtype, nodata, is_cog, has_overviews, raw_json,
            scan_status, error_message, scanned_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            mtime_utc = excluded.mtime_utc,
            format = excluded.format,
            driver = excluded.driver,
            crs = excluded.crs,
            width = excluded.width,
            height = excluded.height,
            bands = excluded.bands,
            dtype = excluded.dtype,
            nodata = excluded.nodata,
            is_cog = excluded.is_cog,
            has_overviews = excluded.has_overviews,
            raw_json = excluded.raw_json,
            scan_status = excluded.scan_status,
            error_message = excluded.error_message,
            scanned_at_utc = excluded.scanned_at_utc
        """,
        (
            record.path,
            record.size_bytes,
            record.mtime_utc,
            record.format,
            record.driver,
            record.crs,
            record.width,
            record.height,
            record.bands,
            record.dtype,
            record.nodata,
            record.is_cog,
            record.has_overviews,
            record.raw_json,
            record.scan_status,
            record.error_message,
            record.scanned_at_utc,
        ),
    )


def print_summary(conn: sqlite3.Connection) -> None:
    queries = {
        "total": "SELECT COUNT(*) FROM files",
        "ok": "SELECT COUNT(*) FROM files WHERE scan_status = 'ok'",
        "error": "SELECT COUNT(*) FROM files WHERE scan_status = 'error'",
        "cog": "SELECT COUNT(*) FROM files WHERE is_cog = 1",
        "not_cog": "SELECT COUNT(*) FROM files WHERE is_cog = 0",
        "missing_crs": "SELECT COUNT(*) FROM files WHERE crs IS NULL OR crs = ''",
    }
    for label, sql in queries.items():
        value = conn.execute(sql).fetchone()[0]
        print(f"{label:12} {value}")


def inventory(
    conn: sqlite3.Connection,
    roots: list[Path],
    extensions: set[str],
    skip_unchanged: bool,
    limit: int | None,
    store_raw_json: bool,
) -> None:
    scanned = 0
    skipped = 0
    for path in iter_candidate_files(roots, extensions):
        if limit is not None and scanned >= limit:
            break

        current_signature = path_signature(path)
        if skip_unchanged:
            existing = get_existing_signature(conn, str(path))
            if existing == current_signature:
                skipped += 1
                continue

        try:
            info = run_gdalinfo(path)
            record = extract_record(path, info, store_raw_json)
            status = "ok"
        except Exception as exc:  # noqa: BLE001
            record = error_record(path, str(exc))
            status = "error"

        upsert_record(conn, record)
        conn.commit()
        scanned += 1
        print(f"[{status}] {path}")

    print(f"done: scanned={scanned} skipped={skipped}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.summary:
        with connect_db(args.db) as conn:
            print_summary(conn)
        return 0

    if not args.roots:
        parser.error("roots are required unless --summary is used")

    ensure_gdalinfo()
    extensions = normalize_extensions(args.extensions)

    with connect_db(args.db) as conn:
        inventory(
            conn=conn,
            roots=args.roots,
            extensions=extensions,
            skip_unchanged=args.skip_unchanged,
            limit=args.limit,
            store_raw_json=args.store_raw_json,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
