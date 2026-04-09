from __future__ import annotations

import importlib
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pytest

from raster_inventory import inventory


def sample_gdal_info() -> dict:
    data = {
        "driverShortName": "GTiff",
        "driverLongName": "GeoTIFF",
        "size": [4, 3],
        "coordinateSystem": {"wkt": "EPSG:4326"},
        "geoTransform": [0.0, 30.0, 0.0, 0.0, 0.0, -30.0],
        "metadata": {
            "": {"AREA_OR_POINT": "Area"},
            "IMAGE_STRUCTURE": {"COMPRESSION": "DEFLATE"},
        },
        "files": ["sample.tif", "sample.ovr"],
        "bands": [
            {
                "band": 1,
                "block": [2, 2],
                "type": "UInt16",
                "colorInterpretation": "Gray",
                "metadata": {
                    "": {
                        "STATISTICS_MINIMUM": "1",
                        "STATISTICS_MAXIMUM": "5",
                        "STATISTICS_MEAN": "2.5",
                        "STATISTICS_STDDEV": "0.5",
                        "NBITS": "12",
                    },
                    "COLOR_TABLE": {"CLASS_1": "background"},
                },
                "overviews": [1],
                "noDataValue": 0,
                "colorTable": {
                    "count": 2,
                    "entries": [[0, 0, 0, 255], [1, 255, 255, 0]],
                },
                "categoryNames": ["water", "land"],
            }
        ],
    }
    # Deep copy to avoid accidental mutation between tests
    return json.loads(json.dumps(data))


class FakeGdal:
    CE_None = 0
    CE_Debug = 1
    CE_Warning = 2
    CE_Failure = 3
    CE_Fatal = 4

    def __init__(self, output, errors: list[tuple[int, str]]):
        self._output = output
        self._errors = errors
        self._handler = None
        self.push_count = 0
        self.pop_count = 0

    def UseExceptions(self) -> None:  # pragma: no cover - unused in tests
        pass

    def PushErrorHandler(self, handler):
        self._handler = handler
        self.push_count += 1

    def PopErrorHandler(self) -> None:
        self._handler = None
        self.pop_count += 1

    def ErrorReset(self) -> None:
        pass

    def Info(self, path: str, format: str = "json"):
        if self._handler is not None:
            for severity, message in self._errors:
                self._handler(severity, 0, message)
        return self._output


@pytest.fixture(autouse=True)
def reset_gdal_module():
    previous = inventory._GDAL_MODULE
    try:
        yield
    finally:
        inventory._GDAL_MODULE = previous


@pytest.fixture
def scratch_dir():
    base = Path(__file__).parent / "tmpdata"
    base.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="inventory-", dir=base))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_warnings_do_not_fail_inventory():
    fake_gdal = FakeGdal(
        output=json.dumps({"driverShortName": "GTiff"}),
        errors=[(FakeGdal.CE_Warning, "metadata mismatch")],
    )
    inventory._GDAL_MODULE = fake_gdal

    info = inventory.run_gdalinfo_python(Path("dummy.tif"))

    assert info["driverShortName"] == "GTiff"
    assert fake_gdal.push_count == 1
    assert fake_gdal.pop_count == 1


def test_failures_raise_runtime_error():
    fake_gdal = FakeGdal(
        output=json.dumps({}),
        errors=[(FakeGdal.CE_Failure, "could not open")],
    )
    inventory._GDAL_MODULE = fake_gdal

    with pytest.raises(RuntimeError, match="could not open"):
        inventory.run_gdalinfo_python(Path("dummy.tif"))


def test_python_info_accepts_dict_output():
    fake_gdal = FakeGdal(output={"driverShortName": "GTiff"}, errors=[])
    inventory._GDAL_MODULE = fake_gdal

    info = inventory.run_gdalinfo_python(Path("dummy.tif"))

    assert info["driverShortName"] == "GTiff"


def test_package_main_delegates_to_inventory_main(monkeypatch):
    module = importlib.import_module("raster_inventory.__main__")
    captured = {}

    def fake_main(argv):
        captured["argv"] = argv
        return 7

    monkeypatch.setattr(module._inventory, "main", fake_main)

    result = module.main(["--flag"])

    assert result == 7
    assert captured["argv"] == ["--flag"]


def test_extract_record_captures_extended_attributes(scratch_dir):
    info = sample_gdal_info()
    raster_path = scratch_dir / "sample.tif"
    raster_path.write_text("dummy")

    payload = inventory.extract_record(raster_path, info, store_raw_json=True)

    assert payload.pixel_width == 30.0
    assert payload.pixel_height == -30.0
    assert payload.block_width == 2
    assert payload.block_height == 2
    assert payload.bits == 12
    assert payload.stats_min == 1.0
    assert payload.color_interp == "Gray"
    assert payload.compression == "DEFLATE"
    assert json.loads(payload.source_files) == ["sample.tif", "sample.ovr"]
    metadata = json.loads(payload.metadata_json)
    assert metadata["IMAGE_STRUCTURE"]["COMPRESSION"] == "DEFLATE"
    band_metadata = json.loads(payload.band_metadata_json)
    assert band_metadata[0][""]["STATISTICS_MAXIMUM"] == "5"
    assert json.loads(payload.color_table)["count"] == 2
    assert json.loads(payload.category_names) == ["water", "land"]


def test_upsert_record_persists_extended_fields(scratch_dir):
    info = sample_gdal_info()
    raster_path = scratch_dir / "sample.tif"
    raster_path.write_text("dummy")
    payload = inventory.extract_record(raster_path, info, store_raw_json=True)

    db_path = scratch_dir / "inventory.sqlite"
    with inventory.connect_db(db_path) as conn:
        inventory.upsert_record(conn, payload)
        conn.commit()

        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT compression, pixel_width, pixel_height, block_width, block_height,
                   bits, stats_min, stats_max, stats_mean, stats_stddev,
                   color_table, category_names, metadata_json, band_metadata_json,
                   source_files
              FROM files
             WHERE path = ?
            """,
            (str(raster_path),),
        ).fetchone()

        assert row["compression"] == "DEFLATE"
        assert row["pixel_width"] == 30.0
        assert row["pixel_height"] == -30.0
        assert row["block_width"] == 2
        assert row["block_height"] == 2
        assert row["bits"] == 12
        assert row["stats_min"] == pytest.approx(1.0)
        assert row["stats_max"] == pytest.approx(5.0)
        assert row["stats_mean"] == pytest.approx(2.5)
        assert row["stats_stddev"] == pytest.approx(0.5)
        assert json.loads(row["color_table"])["count"] == 2
        assert json.loads(row["category_names"]) == ["water", "land"]
        assert json.loads(row["source_files"]) == ["sample.tif", "sample.ovr"]
        metadata = json.loads(row["metadata_json"])
        assert metadata["IMAGE_STRUCTURE"]["COMPRESSION"] == "DEFLATE"
        band_metadata = json.loads(row["band_metadata_json"])
        assert band_metadata[0][""]["STATISTICS_MINIMUM"] == "1"
