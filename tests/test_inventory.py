from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from raster_inventory import inventory


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
