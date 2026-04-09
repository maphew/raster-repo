"""Module entry point for ``python -m raster_inventory``."""

from __future__ import annotations

from typing import Sequence

from . import inventory as _inventory


def main(argv: Sequence[str] | None = None) -> int:
    """Delegate to :mod:`raster_inventory.inventory`."""

    return _inventory.main(list(argv) if argv is not None else None)


if __name__ == "__main__":
    raise SystemExit(main())
