from __future__ import annotations

import json
from typing import Sequence

import pytest

pytest.importorskip("osgeo")
from osgeo import osr  # type: ignore[import-not-found]

from raster_inventory import inventory, stac


def test_stac_generation_from_inventory(scratch_dir, sample_gdal_info):
    db_path = scratch_dir / "inventory.sqlite"
    raster_path = scratch_dir / "tiles" / "sample.tif"
    raster_path.parent.mkdir()
    raster_path.write_text("dummy")

    record = inventory.extract_record(
        raster_path, sample_gdal_info(), store_raw_json=False
    )

    with inventory.connect_db(db_path) as conn:
        inventory.upsert_record(conn, record)
        conn.commit()

    output_dir = scratch_dir / "stac"
    exit_code = stac.main(
        [
            "--db",
            str(db_path),
            "--output",
            str(output_dir),
            "--catalog-id",
            "test-catalog",
        ]
    )

    assert exit_code == 0

    catalog_path = output_dir / "catalog.json"
    assert catalog_path.exists()
    catalog = json.loads(catalog_path.read_text())
    child_links = [link for link in catalog["links"] if link["rel"] == "child"]
    assert len(child_links) == 1

    collection_id = stac.build_collection_id(raster_path.parent)
    collection_path = output_dir / collection_id / "collection.json"
    assert collection_path.exists()
    collection = json.loads(collection_path.read_text())
    assert collection["id"] == collection_id
    assert collection["extent"]["spatial"]["bbox"] == [[0.0, -90.0, 120.0, 0.0]]

    items_dir = collection_path.parent / "items"
    item_files = list(items_dir.glob("*.json"))
    assert len(item_files) == 1
    item = json.loads(item_files[0].read_text())
    assert item["collection"] == collection_id
    assert item["properties"]["datetime"] == record.mtime_utc
    assert item["bbox"] == [0.0, -90.0, 120.0, 0.0]
    assert item["assets"]["data"]["href"] == raster_path.resolve().as_posix()
    assert item["geometry"]["type"] == "Polygon"
    assert item["properties"]["proj:shape"] == [3, 4]
    assert item["properties"]["proj:bbox"] == [0.0, -90.0, 120.0, 0.0]
    assert item["properties"]["proj:transform"] == [0.0, 30.0, 0.0, 0.0, 0.0, -30.0]


def test_stac_main_requires_collection_id_for_single_mode(scratch_dir):
    db_path = scratch_dir / "inventory.sqlite"
    with inventory.connect_db(db_path):
        pass

    with pytest.raises(SystemExit):
        stac.main(
            [
                "--db",
                str(db_path),
                "--output",
                str(scratch_dir / "out"),
                "--group-by",
                "single",
            ]
        )


def test_stac_handles_non_wgs84_inventory(scratch_dir, sample_gdal_info):
    db_path = scratch_dir / "inventory.sqlite"
    raster_path = scratch_dir / "tiles" / "albers.tif"
    raster_path.parent.mkdir()
    raster_path.write_text("dummy")

    info = sample_gdal_info()
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(3578)
    info["coordinateSystem"]["wkt"] = sr.ExportToWkt()
    info["geoTransform"] = [500000.0, 60.0, 0.0, 7200000.0, 0.0, -60.0]
    info["size"] = [2, 2]

    record = inventory.extract_record(
        raster_path,
        info,
        store_raw_json=False,
    )

    with inventory.connect_db(db_path) as conn:
        inventory.upsert_record(conn, record)
        conn.commit()

    output_dir = scratch_dir / "stac"
    exit_code = stac.main(
        [
            "--db",
            str(db_path),
            "--output",
            str(output_dir),
        ]
    )

    assert exit_code == 0

    collection_id = stac.build_collection_id(raster_path.parent)
    item_files = list((output_dir / collection_id / "items").glob("*.json"))
    assert len(item_files) == 1
    item = json.loads(item_files[0].read_text())

    assert item["properties"]["proj:epsg"] == 3578
    assert item["properties"]["proj:bbox"] == [500000.0, 7199880.0, 500120.0, 7200000.0]
    assert item["properties"]["proj:shape"] == [2, 2]
    assert item["properties"]["proj:transform"] == [
        500000.0,
        60.0,
        0.0,
        7200000.0,
        0.0,
        -60.0,
    ]

    expected_bbox, expected_polygon = _project_to_wgs84(
        info["geoTransform"],
        info["size"],
        3578,
    )
    assert item["bbox"] == pytest.approx(expected_bbox)
    assert item["geometry"]["type"] == "Polygon"
    wgs_coords = item["geometry"]["coordinates"][0]
    assert len(wgs_coords) == 5
    for actual, expected in zip(wgs_coords, expected_polygon):
        assert actual == pytest.approx(expected)


def _project_to_wgs84(transform: Sequence[float], size: Sequence[int], epsg: int):
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    transformer = osr.CoordinateTransformation(sr, dst)

    def coords(pixel: int, line: int):
        origin_x, pixel_width, rot_x, origin_y, rot_y, pixel_height = transform
        x = origin_x + pixel * pixel_width + line * rot_x
        y = origin_y + pixel * rot_y + line * pixel_height
        result = transformer.TransformPoint(x, y)
        return float(result[0]), float(result[1])

    width = size[0]
    height = size[1]
    corners = [
        coords(0, 0),
        coords(width, 0),
        coords(width, height),
        coords(0, height),
    ]
    xs = [pt[0] for pt in corners]
    ys = [pt[1] for pt in corners]
    bbox = [min(xs), min(ys), max(xs), max(ys)]
    polygon = [[corner[0], corner[1]] for corner in corners]
    polygon.append(polygon[0])
    return bbox, polygon
