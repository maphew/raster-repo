from __future__ import annotations

import json
import unittest
from pathlib import Path

from raster_inventory import inventory


class FakeGdal:
    CE_None = 0
    CE_Debug = 1
    CE_Warning = 2
    CE_Failure = 3
    CE_Fatal = 4

    def __init__(self, output: dict, errors: list[tuple[int, str]]):
        self._output = json.dumps(output)
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

    def Info(self, path: str, format: str = "json") -> str:  # noqa: A003 - matches GDAL API
        if self._handler is not None:
            for severity, message in self._errors:
                self._handler(severity, 0, message)
        return self._output


class RunGdalInfoPythonTests(unittest.TestCase):
    def tearDown(self) -> None:
        inventory._GDAL_MODULE = None

    def test_warnings_do_not_fail_inventory(self) -> None:
        fake_gdal = FakeGdal(
            output={"driverShortName": "GTiff"},
            errors=[(FakeGdal.CE_Warning, "metadata mismatch")],
        )
        inventory._GDAL_MODULE = fake_gdal

        info = inventory.run_gdalinfo_python(Path("dummy.tif"))

        self.assertEqual(info["driverShortName"], "GTiff")
        self.assertEqual(fake_gdal.push_count, 1)
        self.assertEqual(fake_gdal.pop_count, 1)

    def test_failures_raise_runtime_error(self) -> None:
        fake_gdal = FakeGdal(
            output={}, errors=[(FakeGdal.CE_Failure, "could not open")]
        )
        inventory._GDAL_MODULE = fake_gdal

        with self.assertRaises(RuntimeError) as exc:
            inventory.run_gdalinfo_python(Path("dummy.tif"))

        self.assertIn("could not open", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
