from __future__ import annotations

from raster_inventory import inventory, viewer


def test_view_details_include_extended_metadata(scratch_dir, sample_gdal_info):
    info = sample_gdal_info()
    raster_path = scratch_dir / "sample.tif"
    raster_path.write_text("dummy")
    payload = inventory.extract_record(raster_path, info, store_raw_json=True)

    db_path = scratch_dir / "inventory.sqlite"
    with inventory.connect_db(db_path) as conn:
        inventory.upsert_record(conn, payload)
        conn.commit()

        rows = viewer.fetch_rows(
            conn,
            limit=5,
            status="all",
            search=None,
            order="scanned",
        )

    assert len(rows) == 1

    output = viewer.format_rows(rows, color_enabled=False, details=True)

    assert "px:30x-30" in output
    assert "block:2x2" in output
    assert "bits:12" in output
    assert "compression:DEFLATE" in output
    assert "stats min=1 max=5 mean=2.5 std=0.5" in output
    assert "metadata:" in output and "IMAGE_STRUCTURE" in output
    assert "source_files" in output and "sample.ovr" in output
