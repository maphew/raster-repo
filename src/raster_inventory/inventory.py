from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator
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
    pixel_width REAL,
    pixel_height REAL,
    block_width INTEGER,
    block_height INTEGER,
    bits INTEGER,
    stats_min REAL,
    stats_max REAL,
    stats_mean REAL,
    stats_stddev REAL,
    compression TEXT,
    color_interp TEXT,
    color_table TEXT,
    category_names TEXT,
    metadata_json TEXT,
    band_metadata_json TEXT,
    source_files TEXT,
    overview_count INTEGER,
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


FILE_OPTIONAL_COLUMNS: dict[str, str] = {
    "pixel_width": "REAL",
    "pixel_height": "REAL",
    "block_width": "INTEGER",
    "block_height": "INTEGER",
    "bits": "INTEGER",
    "stats_min": "REAL",
    "stats_max": "REAL",
    "stats_mean": "REAL",
    "stats_stddev": "REAL",
    "compression": "TEXT",
    "color_interp": "TEXT",
    "color_table": "TEXT",
    "category_names": "TEXT",
    "metadata_json": "TEXT",
    "band_metadata_json": "TEXT",
    "source_files": "TEXT",
    "overview_count": "INTEGER",
}


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
    pixel_width: float | None
    pixel_height: float | None
    block_width: int | None
    block_height: int | None
    bits: int | None
    stats_min: float | None
    stats_max: float | None
    stats_mean: float | None
    stats_stddev: float | None
    compression: str | None
    color_interp: str | None
    color_table: str | None
    category_names: str | None
    metadata_json: str | None
    band_metadata_json: str | None
    source_files: str | None
    overview_count: int | None
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
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete the existing database before scanning to force a rebuild.",
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


@contextmanager
def connect_db(db_path: Path) -> Iterator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA_SQL)
    ensure_optional_columns(conn)
    try:
        yield conn
    finally:
        conn.close()


def rebuild_database(db_path: Path) -> None:
    """Remove an existing inventory database and WAL/SHM sidecars."""

    removed: list[Path] = []
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{db_path}{suffix}")
        if candidate.exists():
            candidate.unlink()
            removed.append(candidate)

    if removed:
        removed_paths = ", ".join(str(path) for path in removed)
        print(f"[rebuild] deleted {removed_paths}")


def ensure_optional_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(files)")
    existing = {row[1] for row in cursor.fetchall()}
    for column, column_type in FILE_OPTIONAL_COLUMNS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE files ADD COLUMN {column} {column_type};")


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


@contextmanager
def _capture_gdal_errors(module: ModuleType) -> Iterator[list[tuple[int, str]]]:
    push = getattr(module, "PushErrorHandler", None)
    pop = getattr(module, "PopErrorHandler", None)
    error_reset = getattr(module, "ErrorReset", None)
    if push is None or pop is None or error_reset is None:
        yield []
        return

    collected: list[tuple[int, str]] = []

    def handler(err_class: int, _err_no: int, err_msg: str) -> None:
        try:
            severity = int(err_class)
        except Exception:  # noqa: BLE001
            severity = 0
        collected.append((severity, str(err_msg)))

    error_reset()
    push(handler)
    try:
        yield collected
    finally:
        pop()


def _raise_on_gdal_failures(module: ModuleType, errors: list[tuple[int, str]]) -> None:
    if not errors:
        return
    failure_level = int(getattr(module, "CE_Failure", 3))
    fatal_messages = [msg for severity, msg in errors if severity >= failure_level]
    if fatal_messages:
        raise RuntimeError("; ".join(fatal_messages))


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
        with _capture_gdal_errors(gdal) as errors:
            output = gdal.Info(str(path), format="json")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(str(exc)) from exc
    if not output:
        raise RuntimeError("gdal.Info returned no output")
    _raise_on_gdal_failures(gdal, errors)
    return _coerce_gdal_output(output)


def _coerce_gdal_output(output: Any) -> dict:
    if isinstance(output, dict):
        return output
    if isinstance(output, (bytes, bytearray)):
        output = output.decode("utf-8", errors="replace")
    if isinstance(output, str):
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from gdal.Info: {exc}") from exc
    raise RuntimeError(
        f"Unexpected output type from gdal.Info: {type(output).__name__}"
    )


def _first_band(info: dict) -> dict | None:
    bands = info.get("bands") or []
    if not bands:
        return None
    return bands[0]


def first_band_type(info: dict) -> str | None:
    band = _first_band(info)
    if not band:
        return None
    value = band.get("type")
    return str(value) if value is not None else None


def first_band_nodata(info: dict) -> str | None:
    band = _first_band(info)
    if not band:
        return None
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


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def dataset_metadata(info: dict) -> dict:
    metadata = info.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def band_metadata_list(info: dict) -> list[dict]:
    bands = info.get("bands") or []
    records: list[dict] = []
    for band in bands:
        metadata = band.get("metadata")
        records.append(metadata if isinstance(metadata, dict) else {})
    return records


def dataset_compression(info: dict) -> str | None:
    metadata = dataset_metadata(info)
    image_structure = metadata.get("IMAGE_STRUCTURE")
    if isinstance(image_structure, dict):
        value = image_structure.get("COMPRESSION")
        if value is not None:
            return str(value)
    band = _first_band(info)
    if not band:
        return None
    band_meta = band.get("metadata") or {}
    band_image_structure = band_meta.get("IMAGE_STRUCTURE")
    if isinstance(band_image_structure, dict):
        value = band_image_structure.get("COMPRESSION")
        if value is not None:
            return str(value)
    return None


def pixel_size(info: dict) -> tuple[float | None, float | None]:
    transform = info.get("geoTransform") or []
    pixel_width = parse_float(transform[1]) if len(transform) > 1 else None
    pixel_height = parse_float(transform[5]) if len(transform) > 5 else None
    return pixel_width, pixel_height


def first_band_block(info: dict) -> tuple[int | None, int | None]:
    band = _first_band(info)
    if not band:
        return None, None
    block = band.get("block") or []
    block_width = parse_int(block[0]) if len(block) > 0 else None
    block_height = parse_int(block[1]) if len(block) > 1 else None
    return block_width, block_height


def first_band_bits(info: dict) -> int | None:
    band = _first_band(info)
    if not band:
        return None
    metadata = band.get("metadata") or {}
    default_domain = metadata.get("") or {}
    bits = parse_int(default_domain.get("NBITS"))
    if bits is not None:
        return bits
    dtype = first_band_type(info)
    if dtype:
        digits = "".join(ch for ch in dtype if ch.isdigit())
        number = parse_int(digits) if digits else None
        if number is not None:
            return number
        if dtype.lower() == "byte":
            return 8
    return None


def first_band_stats(
    info: dict,
) -> tuple[float | None, float | None, float | None, float | None]:
    band = _first_band(info)
    if not band:
        return None, None, None, None
    metadata = band.get("metadata") or {}
    default_domain = metadata.get("") or {}
    return (
        parse_float(default_domain.get("STATISTICS_MINIMUM")),
        parse_float(default_domain.get("STATISTICS_MAXIMUM")),
        parse_float(default_domain.get("STATISTICS_MEAN")),
        parse_float(default_domain.get("STATISTICS_STDDEV")),
    )


def encode_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"))


def first_band_color_table(info: dict) -> str | None:
    band = _first_band(info)
    if not band:
        return None
    return encode_json(band.get("colorTable"))


def first_band_category_names(info: dict) -> str | None:
    band = _first_band(info)
    if not band:
        return None
    return encode_json(band.get("categoryNames"))


def first_band_overview_count(info: dict) -> int | None:
    band = _first_band(info)
    if not band:
        return None
    overviews = band.get("overviews") or []
    return len(overviews) if overviews else 0


def dataset_source_files(info: dict) -> str | None:
    files = info.get("files")
    if files is None:
        return None
    return encode_json(files)


def extract_record(path: Path, info: dict, store_raw_json: bool) -> FileRecord:
    size_bytes, mtime_utc = path_signature(path)
    size = info.get("size") or [None, None]
    bands = info.get("bands") or []
    coordinate_system = info.get("coordinateSystem") or {}
    wkt = coordinate_system.get("wkt")
    driver_short = info.get("driverShortName")
    driver_long = info.get("driverLongName")
    pixel_width, pixel_height = pixel_size(info)
    block_width, block_height = first_band_block(info)
    bits = first_band_bits(info)
    stats_min, stats_max, stats_mean, stats_stddev = first_band_stats(info)
    compression = dataset_compression(info)
    color_table = first_band_color_table(info)
    category_names = first_band_category_names(info)
    metadata_json = json.dumps(dataset_metadata(info), separators=(",", ":"))
    band_metadata_json = json.dumps(band_metadata_list(info), separators=(",", ":"))
    source_files = dataset_source_files(info)
    overview_count = first_band_overview_count(info)
    first_band = _first_band(info)
    color_interp = (
        str(first_band.get("colorInterpretation"))
        if first_band and first_band.get("colorInterpretation") is not None
        else None
    )
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
        pixel_width=pixel_width,
        pixel_height=pixel_height,
        block_width=block_width,
        block_height=block_height,
        bits=bits,
        stats_min=stats_min,
        stats_max=stats_max,
        stats_mean=stats_mean,
        stats_stddev=stats_stddev,
        compression=compression,
        color_interp=color_interp,
        color_table=color_table,
        category_names=category_names,
        metadata_json=metadata_json,
        band_metadata_json=band_metadata_json,
        source_files=source_files,
        overview_count=overview_count,
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
        pixel_width=None,
        pixel_height=None,
        block_width=None,
        block_height=None,
        bits=None,
        stats_min=None,
        stats_max=None,
        stats_mean=None,
        stats_stddev=None,
        compression=None,
        color_interp=None,
        color_table=None,
        category_names=None,
        metadata_json=None,
        band_metadata_json=None,
        source_files=None,
        overview_count=None,
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
            bands, dtype, nodata, pixel_width, pixel_height, block_width,
            block_height, bits, stats_min, stats_max, stats_mean, stats_stddev,
            compression, color_interp, color_table, category_names,
            metadata_json, band_metadata_json, source_files, overview_count,
            is_cog, has_overviews, raw_json, scan_status, error_message,
            scanned_at_utc
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?
        )
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
            pixel_width = excluded.pixel_width,
            pixel_height = excluded.pixel_height,
            block_width = excluded.block_width,
            block_height = excluded.block_height,
            bits = excluded.bits,
            stats_min = excluded.stats_min,
            stats_max = excluded.stats_max,
            stats_mean = excluded.stats_mean,
            stats_stddev = excluded.stats_stddev,
            compression = excluded.compression,
            color_interp = excluded.color_interp,
            color_table = excluded.color_table,
            category_names = excluded.category_names,
            metadata_json = excluded.metadata_json,
            band_metadata_json = excluded.band_metadata_json,
            source_files = excluded.source_files,
            overview_count = excluded.overview_count,
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
            record.pixel_width,
            record.pixel_height,
            record.block_width,
            record.block_height,
            record.bits,
            record.stats_min,
            record.stats_max,
            record.stats_mean,
            record.stats_stddev,
            record.compression,
            record.color_interp,
            record.color_table,
            record.category_names,
            record.metadata_json,
            record.band_metadata_json,
            record.source_files,
            record.overview_count,
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.summary and args.rebuild:
        parser.error("--rebuild cannot be combined with --summary")

    if args.summary:
        with connect_db(args.db) as conn:
            print_summary(conn)
        return 0

    if not args.roots:
        parser.error("roots are required unless --summary is used")

    if args.rebuild:
        rebuild_database(args.db)

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
