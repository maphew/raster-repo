"""Generate STAC Collections and Items from an inventory database."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Iterable, Sequence

from .inventory import connect_db


STAC_VERSION = "1.0.0"
CATALOG_DEFAULT_ID = "raster-inventory"
CATALOG_DEFAULT_DESCRIPTION = "STAC catalog generated from raster inventory records."
DEFAULT_SPATIAL_EXTENT = [-180.0, -90.0, 180.0, 90.0]
_OSR_MODULE: ModuleType | None = None


@dataclass(slots=True)
class InventoryRow:
    path: str
    size_bytes: int | None
    mtime_utc: str | None
    scanned_at_utc: str | None
    width: int | None
    height: int | None
    bands: int | None
    dtype: str | None
    crs: str | None
    geo_transform_json: str | None
    is_cog: int | None


@dataclass(slots=True)
class ExtentStats:
    minx: float | None = None
    miny: float | None = None
    maxx: float | None = None
    maxy: float | None = None
    start: str | None = None
    end: str | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate STAC Collections and Items from inventory records."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("inventory.sqlite"),
        help="Path to the inventory SQLite database. Default: inventory.sqlite",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Directory where STAC JSON files will be written.",
    )
    parser.add_argument(
        "--group-by",
        choices=("parent", "single"),
        default="parent",
        help="Group rasters into collections by parent directory or emit a single collection.",
    )
    parser.add_argument(
        "--collection-id",
        help="Collection id to use when --group-by single is selected.",
    )
    parser.add_argument(
        "--collection-title",
        help="Optional title for the single collection.",
    )
    parser.add_argument(
        "--collection-description",
        help=(
            "Optional description. For parent grouping this acts as a template and may include "
            "{group} which will be replaced with the source directory."
        ),
    )
    parser.add_argument(
        "--catalog-id",
        default=CATALOG_DEFAULT_ID,
        help=f"Root catalog id. Default: {CATALOG_DEFAULT_ID}",
    )
    parser.add_argument(
        "--catalog-description",
        default=CATALOG_DEFAULT_DESCRIPTION,
        help="Root catalog description.",
    )
    parser.add_argument(
        "--asset-prefix",
        help="Prefix to prepend to asset hrefs (e.g. s3://bucket/path).",
    )
    parser.add_argument(
        "--license",
        default="proprietary",
        help="Collection license string. Default: proprietary",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of inventory rows processed (for testing).",
    )
    parser.add_argument(
        "--allow-non-wgs84",
        action="store_true",
        help="Allow geometry emission even when CRS is not WGS84 (not recommended).",
    )
    return parser


def load_inventory_rows(
    conn: sqlite3.Connection, limit: int | None
) -> list[InventoryRow]:
    conn.row_factory = sqlite3.Row
    sql = [
        "SELECT path, size_bytes, mtime_utc, scanned_at_utc, width, height,",
        "       bands, dtype, crs, geo_transform_json, is_cog",
        "  FROM files",
        " WHERE scan_status = 'ok'",
        " ORDER BY path",
    ]
    params: list[object] = []
    if limit is not None:
        sql.append(" LIMIT ?")
        params.append(limit)
    cursor = conn.execute("".join(sql), params)
    rows: list[InventoryRow] = []
    for row in cursor.fetchall():
        rows.append(
            InventoryRow(
                path=str(row["path"]),
                size_bytes=_maybe_int(row["size_bytes"]),
                mtime_utc=_maybe_str(row["mtime_utc"]),
                scanned_at_utc=_maybe_str(row["scanned_at_utc"]),
                width=_maybe_int(row["width"]),
                height=_maybe_int(row["height"]),
                bands=_maybe_int(row["bands"]),
                dtype=_maybe_str(row["dtype"]),
                crs=_maybe_str(row["crs"]),
                geo_transform_json=_maybe_str(row["geo_transform_json"]),
                is_cog=_maybe_int(row["is_cog"]),
            )
        )
    return rows


def _maybe_int(value: int | float | str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _maybe_str(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    return str(value)  # type: ignore[arg-type]


def slugify(value: str) -> str:
    lowered = value.lower()
    sanitized = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return sanitized or "collection"


def detect_epsg(crs: str | None) -> int | None:
    if not crs:
        return None
    matches = re.findall(r"epsg[^0-9]*(\d+)", crs, flags=re.IGNORECASE)
    if matches:
        return int(matches[-1])
    return None


def is_wgs84(crs: str | None) -> bool:
    if not crs:
        return False
    lowered = crs.lower()
    return "4326" in lowered or "wgs84" in lowered or "wgs 84" in lowered


def parse_transform(
    value: str | None,
) -> tuple[float, float, float, float, float, float] | None:
    if not value:
        return None
    try:
        data = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, list) or len(data) < 6:
        return None
    try:
        return (
            float(data[0]),
            float(data[1]),
            float(data[2]),
            float(data[3]),
            float(data[4]),
            float(data[5]),
        )
    except (TypeError, ValueError):
        return None


def _encode_json(value: object | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"))


def load_runtime_metadata(path: Path) -> dict | None:
    try:
        from .inventory import run_gdalinfo

        return run_gdalinfo(path)
    except Exception:  # noqa: BLE001
        return None


def resolve_row_metadata(row: InventoryRow) -> tuple[InventoryRow, dict | None]:
    needs_runtime_lookup = (
        row.geo_transform_json is None
        or row.width is None
        or row.height is None
        or row.crs is None
    )
    if not needs_runtime_lookup:
        return row, None

    runtime_info = load_runtime_metadata(Path(row.path))
    if runtime_info is None:
        return row, None

    size = runtime_info.get("size") or []
    coordinate_system = runtime_info.get("coordinateSystem") or {}
    return (
        replace(
            row,
            width=row.width
            if row.width is not None
            else _maybe_int(size[0] if len(size) > 0 else None),
            height=row.height
            if row.height is not None
            else _maybe_int(size[1] if len(size) > 1 else None),
            crs=row.crs or _maybe_str(coordinate_system.get("wkt")),
            geo_transform_json=row.geo_transform_json
            or _encode_json(runtime_info.get("geoTransform")),
        ),
        runtime_info,
    )


def pixel_to_coords(
    transform: tuple[float, float, float, float, float, float],
    pixel: int,
    line: int,
) -> tuple[float, float]:
    origin_x, pixel_width, rot_x, origin_y, rot_y, pixel_height = transform
    x = origin_x + pixel * pixel_width + line * rot_x
    y = origin_y + pixel * rot_y + line * pixel_height
    return x, y


def _corners_from_runtime_info(
    runtime_info: dict | None,
) -> list[tuple[float, float]] | None:
    if runtime_info is None:
        return None
    corner_coordinates = runtime_info.get("cornerCoordinates") or {}
    ordered_keys = ("upperLeft", "upperRight", "lowerRight", "lowerLeft")
    points: list[tuple[float, float]] = []
    for key in ordered_keys:
        value = corner_coordinates.get(key)
        if not isinstance(value, list) or len(value) < 2:
            return None
        try:
            points.append((float(value[0]), float(value[1])))
        except (TypeError, ValueError):
            return None
    return points


def compute_geometry(
    row: InventoryRow,
    *,
    allow_native_geometry: bool,
    runtime_info: dict | None = None,
) -> tuple[dict, list[float], list[float], list[float] | None] | None:
    transform = parse_transform(row.geo_transform_json)
    native_transform: list[float] | None = None
    if transform is not None and row.width is not None and row.height is not None:
        width = int(row.width)
        height = int(row.height)
        native_corners = _corner_points(transform, width, height)
        native_transform = list(transform)
    else:
        native_corners = _corners_from_runtime_info(runtime_info)
        if native_corners is None:
            return None

    native_bbox = _bbox_from_corners(native_corners)
    wgs_corners = _reproject_corners(native_corners, row.crs)

    if wgs_corners is None:
        if allow_native_geometry:
            wgs_corners = native_corners
        else:
            return None

    wgs_polygon = _polygon_from_corners(wgs_corners)
    wgs_bbox = _bbox_from_corners(wgs_corners)
    return wgs_polygon, wgs_bbox, native_bbox, native_transform


def _corner_points(
    transform: tuple[float, float, float, float, float, float], width: int, height: int
) -> list[tuple[float, float]]:
    return [
        pixel_to_coords(transform, 0, 0),
        pixel_to_coords(transform, width, 0),
        pixel_to_coords(transform, width, height),
        pixel_to_coords(transform, 0, height),
    ]


def _bbox_from_corners(points: list[tuple[float, float]]) -> list[float]:
    xs = [pt[0] for pt in points]
    ys = [pt[1] for pt in points]
    return [min(xs), min(ys), max(xs), max(ys)]


def _polygon_from_corners(points: list[tuple[float, float]]) -> dict:
    ring = [[x, y] for x, y in points]
    ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}


def _import_osr() -> ModuleType:
    global _OSR_MODULE
    if _OSR_MODULE is None:
        from osgeo import osr as module  # type: ignore[import-not-found]

        _OSR_MODULE = module
    return _OSR_MODULE


def _build_spatial_reference(crs: str | None, epsg: int | None):
    osr = _import_osr()
    if crs:
        sr = osr.SpatialReference()
        try:
            if sr.ImportFromWkt(crs) == 0:
                return sr
        except Exception:  # noqa: BLE001
            pass
    if epsg is not None:
        sr = osr.SpatialReference()
        if sr.ImportFromEPSG(epsg) == 0:
            return sr
    return None


def _reproject_corners(
    points: list[tuple[float, float]], crs: str | None
) -> list[tuple[float, float]] | None:
    if is_wgs84(crs):
        return points
    epsg = detect_epsg(crs)
    src = _build_spatial_reference(crs, epsg)
    if src is None:
        return None
    osr = _import_osr()
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    transformer = osr.CoordinateTransformation(src, dst)
    transformed: list[tuple[float, float]] = []
    for x, y in points:
        try:
            result = transformer.TransformPoint(x, y)
        except Exception:  # noqa: BLE001
            return None
        transformed.append((float(result[0]), float(result[1])))
    return transformed


def build_asset_href(path: Path, prefix: str | None) -> str:
    if prefix:
        clean = prefix.rstrip("/")
        return f"{clean}/{path.name}"
    return path.resolve().as_posix()


def build_collection_id(parent: Path) -> str:
    label = parent.name or parent.as_posix() or "root"
    slug = slugify(label)
    digest = hashlib.sha1(str(parent).encode("utf-8")).hexdigest()[:8]
    return f"{slug}-{digest}"


def item_identifier(path: Path) -> str:
    slug = slugify(path.stem or "item")
    digest = hashlib.sha1(str(path).encode("utf-8")).hexdigest()[:8]
    return f"{slug}-{digest}"


def normalize_datetime(value: str | None) -> str | None:
    if not value:
        return None
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return None
    return value


def update_extent(extent: ExtentStats, bbox: list[float], when: str | None) -> None:
    minx, miny, maxx, maxy = bbox
    extent.minx = min(minx, extent.minx) if extent.minx is not None else minx
    extent.miny = min(miny, extent.miny) if extent.miny is not None else miny
    extent.maxx = max(maxx, extent.maxx) if extent.maxx is not None else maxx
    extent.maxy = max(maxy, extent.maxy) if extent.maxy is not None else maxy
    if when:
        if extent.start is None or when < extent.start:
            extent.start = when
        if extent.end is None or when > extent.end:
            extent.end = when


def generate_stac(
    rows: Iterable[InventoryRow],
    *,
    output: Path,
    group_by: str,
    collection_id: str | None,
    collection_title: str | None,
    collection_description: str | None,
    catalog_id: str,
    catalog_description: str,
    asset_prefix: str | None,
    license_name: str,
    allow_non_wgs84: bool,
) -> tuple[int, int, list[tuple[str, str]]]:
    output.mkdir(parents=True, exist_ok=True)
    catalog_path = output / "catalog.json"
    collection_map: dict[str, dict] = {}
    extent_map: dict[str, ExtentStats] = {}
    items_by_collection: defaultdict[str, list[Path]] = defaultdict(list)
    skipped: list[tuple[str, str]] = []

    for row in rows:
        row, runtime_info = resolve_row_metadata(row)
        geometry = compute_geometry(
            row,
            allow_native_geometry=allow_non_wgs84,
            runtime_info=runtime_info,
        )
        item_geometry: dict | None
        bbox: list[float] | None
        native_bbox: list[float] | None
        native_transform: list[float] | None
        if geometry is None and row.width is not None and row.height is not None:
            item_geometry = None
            bbox = None
            native_bbox = None
            parsed_transform = parse_transform(row.geo_transform_json)
            native_transform = (
                list(parsed_transform) if parsed_transform is not None else None
            )
        elif geometry is None:
            reason = "missing geometry"
            if row.geo_transform_json is None:
                reason = "missing transform"
            elif row.width is None or row.height is None:
                reason = "missing dimensions"
            else:
                reason = "reprojection failed"
            skipped.append((row.path, reason))
            continue
        else:
            item_geometry, bbox, native_bbox, native_transform = geometry

        parent = Path(row.path).parent
        if group_by == "single":
            if not collection_id:
                raise ValueError("collection_id is required for single group mode")
            current_collection_id = collection_id
            title = collection_title or collection_id
            description = collection_description or catalog_description
        else:
            current_collection_id = build_collection_id(parent)
            label = parent.as_posix() or "root"
            title = parent.name or label
            template = collection_description or "Inventory group for {group}"
            description = template.format(group=label)

        info = collection_map.setdefault(
            current_collection_id,
            {
                "title": title,
                "description": description,
                "path": parent,
            },
        )
        extent = extent_map.setdefault(current_collection_id, ExtentStats())

        when = normalize_datetime(row.mtime_utc) or normalize_datetime(
            row.scanned_at_utc
        )
        if bbox is not None:
            update_extent(extent, bbox, when)
        elif when:
            if extent.start is None or when < extent.start:
                extent.start = when
            if extent.end is None or when > extent.end:
                extent.end = when

        item_id = item_identifier(Path(row.path))
        item_path = output / current_collection_id / "items" / f"{item_id}.json"
        items_by_collection[current_collection_id].append(item_path)

        epsg = detect_epsg(row.crs)
        href = build_asset_href(Path(row.path), asset_prefix)
        properties = {
            "datetime": when,
            "created": normalize_datetime(row.scanned_at_utc),
            "file:size": row.size_bytes,
        }
        if row.dtype:
            properties["raster:dtype"] = row.dtype
        if epsg is not None:
            properties["proj:epsg"] = epsg
        if row.is_cog is not None:
            properties["raster:cog"] = bool(row.is_cog)
        if row.bands is not None:
            properties["raster:bands"] = row.bands
        if row.height is not None and row.width is not None:
            properties["proj:shape"] = [int(row.height), int(row.width)]
        if native_bbox:
            properties["proj:bbox"] = native_bbox
        if native_transform:
            properties["proj:transform"] = native_transform

        item = {
            "type": "Feature",
            "stac_version": STAC_VERSION,
            "stac_extensions": [],
            "id": item_id,
            "bbox": bbox,
            "geometry": item_geometry,
            "properties": properties,
            "collection": current_collection_id,
            "assets": {
                "data": {
                    "href": href,
                    "title": Path(row.path).name,
                    "roles": ["data"],
                    "type": "image/tiff; application=geotiff",
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": (
                        Path(current_collection_id) / "items" / f"{item_id}.json"
                    ).as_posix(),
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "../collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "../../catalog.json",
                    "type": "application/json",
                },
            ],
        }

        _write_json(item_path, item)

    collection_count = 0
    for collection_key, info in collection_map.items():
        extent = extent_map[collection_key]
        collection_dir = output / collection_key
        collection_dir.mkdir(parents=True, exist_ok=True)
        items_dir = collection_dir / "items"
        items_dir.mkdir(exist_ok=True)

        spatial_bbox = (
            [extent.minx, extent.miny, extent.maxx, extent.maxy]
            if extent.minx is not None
            and extent.miny is not None
            and extent.maxx is not None
            and extent.maxy is not None
            else DEFAULT_SPATIAL_EXTENT
        )
        collection_bbox = [spatial_bbox]
        interval = [[extent.start, extent.end]]
        collection_rel = (Path(collection_key) / "collection.json").as_posix()
        links = [
            {
                "rel": "self",
                "href": collection_rel,
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "../catalog.json",
                "type": "application/json",
            },
            {
                "rel": "parent",
                "href": "../catalog.json",
                "type": "application/json",
            },
        ]

        for item_path in items_by_collection.get(collection_key, []):
            links.append(
                {
                    "rel": "item",
                    "href": item_path.relative_to(output).as_posix(),
                    "type": "application/geo+json",
                }
            )

        collection_doc = {
            "type": "Collection",
            "stac_version": STAC_VERSION,
            "id": collection_key,
            "description": info["description"],
            "title": info["title"],
            "license": license_name,
            "extent": {
                "spatial": {"bbox": collection_bbox},
                "temporal": {"interval": interval},
            },
            "links": links,
        }
        _write_json(collection_dir / "collection.json", collection_doc)
        collection_count += 1

    catalog_links = [
        {
            "rel": "self",
            "href": "catalog.json",
            "type": "application/json",
        },
    ]
    for collection_key, info in collection_map.items():
        catalog_links.append(
            {
                "rel": "child",
                "href": (Path(collection_key) / "collection.json").as_posix(),
                "type": "application/json",
                "title": info["title"],
            }
        )

    catalog = {
        "type": "Catalog",
        "stac_version": STAC_VERSION,
        "id": catalog_id,
        "description": catalog_description,
        "links": catalog_links,
    }
    _write_json(catalog_path, catalog)

    total_items = sum(len(v) for v in items_by_collection.values())
    return collection_count, total_items, skipped


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.group_by == "single" and not args.collection_id:
        parser.error("--collection-id is required when --group-by single")

    with connect_db(args.db) as conn:
        rows = load_inventory_rows(conn, args.limit)

    collections, items, skipped = generate_stac(
        rows,
        output=args.output,
        group_by=args.group_by,
        collection_id=args.collection_id,
        collection_title=args.collection_title,
        collection_description=args.collection_description,
        catalog_id=args.catalog_id,
        catalog_description=args.catalog_description,
        asset_prefix=args.asset_prefix,
        license_name=args.license,
        allow_non_wgs84=args.allow_non_wgs84,
    )

    print(
        f"wrote {items} items across {collections} collections (skipped {len(skipped)} records)"
    )
    for path, reason in skipped:
        print(f"[skip] {path}: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
