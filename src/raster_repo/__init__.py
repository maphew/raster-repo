"""Console script entry point for raster-repo."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

from raster_inventory.inventory import main as inventory_main
from raster_inventory.stac import main as stac_main
from raster_inventory.viewer import main as view_main


def _print_help(prog: str) -> int:
    message = f"""usage: {prog} <command> [options]\n\n"""
    message += "Commands:\n"
    message += (
        "  inventory   Scan raster roots into SQLite (default when no command).\n"
    )
    message += "  view        Inspect existing inventory rows without rescanning.\n"
    message += "  stac        Emit STAC catalog/collections/items from the inventory.\n"
    message += "\nUse '{prog} <command> --help' for command-specific options.".format(
        prog=prog
    )
    print(message)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch to either the inventory or view sub-command."""

    args = list(argv) if argv is not None else sys.argv[1:]

    if args:
        command = args[0]
        remaining = args[1:]
        if command in {"-h", "--help"}:
            return _print_help(_program_name(argv))
        if command == "view":
            return view_main(remaining)
        if command == "inventory":
            return inventory_main(remaining)
        if command == "stac":
            return stac_main(remaining)

    return inventory_main(args)


def _program_name(argv: Sequence[str] | None) -> str:
    if argv and len(argv) > 0:
        return Path(argv[0]).name
    return Path(sys.argv[0]).name
