"""Console script entry that dispatches to the inventory CLI."""

from __future__ import annotations

from raster_inventory.inventory import main as inventory_main


def main() -> int:
    """Invoke the raster inventory CLI."""

    return inventory_main()
