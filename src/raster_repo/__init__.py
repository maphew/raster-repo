"""Console script entry point for raster-repo."""

from __future__ import annotations

import sys
from typing import Sequence

from raster_inventory.inventory import main as inventory_main
from raster_inventory.viewer import main as view_main


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch to either the inventory or view sub-command."""

    args = list(argv) if argv is not None else sys.argv[1:]

    if args:
        command = args[0]
        remaining = args[1:]
        if command == "view":
            return view_main(remaining)
        if command == "inventory":
            return inventory_main(remaining)

    return inventory_main(args)
