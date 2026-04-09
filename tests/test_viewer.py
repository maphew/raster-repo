from __future__ import annotations

from raster_inventory import inventory, viewer


def _seed_inventory_record(scratch_dir, sample_gdal_info):
    info = sample_gdal_info()
    raster_path = scratch_dir / "sample.tif"
    raster_path.write_text("dummy")
    payload = inventory.extract_record(raster_path, info, store_raw_json=True)

    db_path = scratch_dir / "inventory.sqlite"
    with inventory.connect_db(db_path) as conn:
        inventory.upsert_record(conn, payload)
        conn.commit()
        record_id = int(
            conn.execute(
                "SELECT id FROM files WHERE path = ?",
                (str(raster_path),),
            ).fetchone()[0]
        )
    return db_path, record_id, raster_path


def test_view_details_include_extended_metadata(scratch_dir, sample_gdal_info):
    db_path, _, _ = _seed_inventory_record(scratch_dir, sample_gdal_info)

    with inventory.connect_db(db_path) as conn:
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


def test_lookup_rows_by_identifier_accepts_numeric_id(scratch_dir, sample_gdal_info):
    db_path, record_id, _ = _seed_inventory_record(scratch_dir, sample_gdal_info)

    with inventory.connect_db(db_path) as conn:
        rows = viewer.lookup_rows_by_identifier(conn, str(record_id))

    assert len(rows) == 1
    assert rows[0]["id"] == record_id


def test_lookup_rows_by_identifier_accepts_name(scratch_dir, sample_gdal_info):
    db_path, record_id, raster_path = _seed_inventory_record(
        scratch_dir, sample_gdal_info
    )

    with inventory.connect_db(db_path) as conn:
        rows = viewer.lookup_rows_by_identifier(conn, raster_path.name)

    assert len(rows) == 1
    assert rows[0]["id"] == record_id


def test_suggest_similar_names_returns_candidates(scratch_dir, sample_gdal_info):
    db_path, record_id, raster_path = _seed_inventory_record(
        scratch_dir, sample_gdal_info
    )

    with inventory.connect_db(db_path) as conn:
        suggestions = viewer.suggest_similar_names(conn, "simple.tif")

    assert (record_id, raster_path.name) in suggestions


def test_view_identifier_outputs_details_without_flag(
    scratch_dir, sample_gdal_info, capsys
):
    db_path, _, raster_path = _seed_inventory_record(scratch_dir, sample_gdal_info)

    exit_code = viewer.main(
        [
            "--db",
            str(db_path),
            "--no-color",
            raster_path.name,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "px:30x-30" in captured.out
