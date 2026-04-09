from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

from raster_inventory.inventory import connect_db


STATUS_COLORS: dict[str, str] = {
    "ok": "\x1b[32m",  # green
    "error": "\x1b[31m",  # red
}


VIEW_COLUMNS = (
    "path",
    "mtime_utc",
    "dtype",
    "bands",
    "width",
    "height",
    "size_bytes",
    "scan_status",
    "scanned_at_utc",
    "pixel_width",
    "pixel_height",
    "block_width",
    "block_height",
    "bits",
    "stats_min",
    "stats_max",
    "stats_mean",
    "stats_stddev",
    "compression",
    "color_interp",
    "color_table",
    "category_names",
    "metadata_json",
    "band_metadata_json",
    "source_files",
    "overview_count",
    "is_cog",
    "has_overviews",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect existing raster inventory records."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("inventory.sqlite"),
        help="SQLite database path. Default: inventory.sqlite",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of rows to display. Default: 20",
    )
    parser.add_argument(
        "--status",
        choices=("ok", "error", "all"),
        default="all",
        help="Filter by scan status. Default: all",
    )
    parser.add_argument(
        "--search",
        help="Case-insensitive substring to match against the file path.",
    )
    parser.add_argument(
        "--order",
        choices=("scanned", "mtime", "path", "size"),
        default="scanned",
        help="Sort rows by scanned time, mtime, path, or file size.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors in the table output.",
    )
    parser.add_argument(
        "--pager",
        action="store_true",
        help="Pipe the output through less -R (when available).",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Show extended metadata for each row.",
    )
    return parser


def fetch_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    status: str,
    search: str | None,
    order: str,
) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    clauses: list[str] = []
    params: list[object] = []

    if status != "all":
        clauses.append("scan_status = ?")
        params.append(status)

    if search:
        clauses.append("LOWER(path) LIKE ?")
        params.append(f"%{search.lower()}%")

    order_by = {
        "scanned": "datetime(scanned_at_utc) DESC",
        "mtime": "datetime(mtime_utc) DESC",
        "path": "path COLLATE NOCASE ASC",
        "size": "size_bytes DESC",
    }[order]

    columns = ", ".join(VIEW_COLUMNS)
    sql = [f"SELECT {columns}", "  FROM files"]
    if clauses:
        sql.append(" WHERE " + " AND ".join(clauses))
    sql.append(f" ORDER BY {order_by}")
    if limit > 0:
        sql.append(" LIMIT ?")
        params.append(limit)

    cursor = conn.execute("".join(sql), params)
    return list(cursor.fetchall())


def format_rows(
    rows: list[sqlite3.Row], *, color_enabled: bool, details: bool = False
) -> str:
    if not rows:
        return "No records found in the inventory."

    term_width = shutil.get_terminal_size(fallback=(120, 24)).columns
    status_width = 8
    name_width = 26
    date_width = 17
    type_width = 14
    bands_width = 5
    size_width = 13
    gap = 2

    fixed = (
        status_width
        + name_width
        + date_width
        + type_width
        + bands_width
        + size_width
        + gap * 6
    )
    path_width = max(24, term_width - fixed)

    headers = (
        f"{'status':<{status_width}}"
        f"{' ' * gap}{'name':<{name_width}}"
        f"{' ' * gap}{'date':<{date_width}}"
        f"{' ' * gap}{'type':<{type_width}}"
        f"{' ' * gap}{'bands':>{bands_width}}"
        f"{' ' * gap}{'size':>{size_width}}"
        f"{' ' * gap}{'path':<{path_width}}"
    )

    lines = [headers, "-" * min(term_width, len(headers))]
    for row in rows:
        status = str(row["scan_status"]) if row["scan_status"] is not None else "-"
        name = Path(str(row["path"]))
        name_text = truncate(name.name, name_width)
        date_text = format_date(row["mtime_utc"])
        dtype_text = format_type(row["dtype"])
        bands_text = format_int(row["bands"])
        size_text = format_dimensions(row["width"], row["height"])
        path_text = truncate(str(row["path"]), path_width)

        status_field = f"{status:<{status_width}}"
        if color_enabled:
            status_field = colorize(status, status_field)

        line = (
            f"{status_field}"
            f"{' ' * gap}{name_text:<{name_width}}"
            f"{' ' * gap}{date_text:<{date_width}}"
            f"{' ' * gap}{dtype_text:<{type_width}}"
            f"{' ' * gap}{bands_text:>{bands_width}}"
            f"{' ' * gap}{size_text:>{size_width}}"
            f"{' ' * gap}{path_text:<{path_width}}"
        )
        lines.append(line)

        if details:
            lines.extend(format_detail_lines(row))

    return "\n".join(lines)


def format_date(value: str | None) -> str:
    if not value:
        return "-"
    display = value.replace("T", " ")
    return display[:16]


def format_type(dtype: str | None) -> str:
    if not dtype:
        return "-"
    bits = dtype_bits(dtype)
    if bits:
        return f"{dtype}/{bits}"
    return dtype


def format_int(value: int | float | str | None) -> str:
    if value is None:
        return "-"
    try:
        return f"{int(value)}"
    except (TypeError, ValueError):
        return "-"


def format_dimensions(
    width: int | float | str | None, height: int | float | str | None
) -> str:
    if width is None or height is None:
        return "-"
    try:
        w = int(width)
        h = int(height)
    except (TypeError, ValueError):
        return "-"
    return f"{w:,}x{h:,}"


def format_float_value(value: float | int | str | None) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    text = f"{number:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def format_bool_flag(value: float | int | str | None) -> str:
    if value is None:
        return "?"
    try:
        return "yes" if int(value) else "no"
    except (TypeError, ValueError):
        return "?"


def format_json_preview(
    value: str | bytes | None, *, max_length: int = 200
) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace").strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        formatted = text
    else:
        formatted = json.dumps(parsed, separators=(",", ":"))
    if len(formatted) > max_length:
        return f"{formatted[: max_length - 3]}..."
    return formatted


def format_detail_lines(row: sqlite3.Row) -> list[str]:
    indent = " " * 4
    px_text = f"{format_float_value(row['pixel_width'])}x{format_float_value(row['pixel_height'])}"
    block_text = f"{format_int(row['block_width'])}x{format_int(row['block_height'])}"
    first_line = (
        f"{indent}px:{px_text}"
        f"  block:{block_text}"
        f"  bits:{format_int(row['bits'])}"
        f"  dtype:{format_type(row['dtype'])}"
        f"  compression:{row['compression'] or '-'}"
        f"  color:{row['color_interp'] or '-'}"
        f"  cog:{format_bool_flag(row['is_cog'])}"
        f"  overviews:{format_int(row['overview_count'])}"
        f"  has_overviews:{format_bool_flag(row['has_overviews'])}"
    )

    stats_line = (
        f"{indent}stats min={format_float_value(row['stats_min'])}"
        f" max={format_float_value(row['stats_max'])}"
        f" mean={format_float_value(row['stats_mean'])}"
        f" std={format_float_value(row['stats_stddev'])}"
    )

    lines = [first_line.rstrip(), stats_line.rstrip()]

    for label, key in (
        ("metadata", "metadata_json"),
        ("band_metadata", "band_metadata_json"),
        ("color_table", "color_table"),
        ("category_names", "category_names"),
        ("source_files", "source_files"),
    ):
        preview = format_json_preview(row[key])
        if preview:
            lines.append(f"{indent}{label}: {preview}")

    return lines


def dtype_bits(dtype: str) -> str:
    digits = "".join(ch for ch in dtype if ch.isdigit())
    if digits:
        return f"{digits}b"
    if dtype.lower() == "byte":
        return "8b"
    return ""


def truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def colorize(status: str, value: str) -> str:
    color = STATUS_COLORS.get(status.lower())
    if not color:
        return value
    reset = "\x1b[0m"
    return f"{color}{value}{reset}"


def emit_output(text: str, *, use_pager: bool) -> None:
    if use_pager and sys.stdout.isatty():
        pager = shutil.which("less") or shutil.which("more")
        if pager:
            args = [pager]
            if Path(pager).name == "less":
                args.append("-R")
            subprocess.run(args, input=text, text=True, check=False)
            return
    print(text)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    color_enabled = (not args.no_color) and (sys.stdout.isatty() or args.pager)

    with connect_db(args.db) as conn:
        rows = fetch_rows(
            conn,
            limit=args.limit,
            status=args.status,
            search=args.search,
            order=args.order,
        )

    output = format_rows(rows, color_enabled=color_enabled, details=args.details)
    emit_output(output, use_pager=args.pager)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
