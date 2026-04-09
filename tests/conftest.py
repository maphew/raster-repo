from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def sample_gdal_info():
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

    def factory() -> dict:
        return json.loads(json.dumps(data))

    return factory


@pytest.fixture
def scratch_dir():
    base = Path(__file__).parent / "tmpdata"
    base.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="inventory-", dir=base))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
