"""Index geotagged photos recursively using QGIS, then attribute source folders.

_codex-gpt-6.1-sol- on behalf of Matt Wilkie_
"""
from __future__ import annotations

import argparse
from contextlib import closing
import fnmatch
import math
import os
from pathlib import Path
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from xml.etree import ElementTree as ET


def match_folders(pattern: str, root: Path) -> list[Path]:
    """Prefer a literal directory; otherwise match immediate child names."""
    literal = Path(pattern).expanduser()
    if not literal.is_absolute():
        literal = root / literal
    if literal.is_dir():
        return [literal.resolve()]
    return sorted(
        (p.resolve() for p in root.iterdir()
         if p.is_dir() and fnmatch.fnmatchcase(p.name.casefold(), pattern.casefold())),
        key=lambda p: p.name.casefold(),
    )


def attribute_sources(path: Path) -> None:
    """Add containing-folder name and absolute directory path to photo points.

    Use OGR so GeoPackage geometry, spatial indexes and triggers remain valid.
    """
    try:
        from osgeo import ogr
    except ImportError as exc:
        raise RuntimeError('GDAL Python bindings are required; run with uv run.') from exc

    with ogr.ExceptionMgr():
        with ogr.Open(str(path), update=1) as ds:
            if ds is None or ds.GetLayerCount() != 1:
                raise RuntimeError(f'Expected one photo layer in {path}')
            layer = ds.GetLayer(0)
            if layer.GetLayerDefn().GetFieldIndex('photo') < 0:
                raise RuntimeError(f'Missing photo attribute in {path}')
            layer.CreateField(ogr.FieldDefn('source_folder', ogr.OFTString))
            layer.CreateField(ogr.FieldDefn('source_path', ogr.OFTString))
            layer.StartTransaction()
            try:
                for feature in layer:
                    photo = feature.GetField('photo')
                    if not photo or not Path(photo).is_absolute():
                        raise RuntimeError(f'Expected absolute photo path, got {photo!r}')
                    parent = Path(photo).parent
                    feature.SetField('source_folder', parent.name)
                    feature.SetField('source_path', str(parent))
                    layer.SetFeature(feature)
                layer.CommitTransaction()
            except BaseException:
                layer.RollbackTransaction()
                raise


# Output name, GDAL EXIF tag suffix, storage type.
# _codex-gpt-6.1-sol- on behalf of Matt Wilkie_
EXIF_FIELDS = (
    ('OffsetTimeOriginal', 'OffsetTimeOriginal', str),
    ('SubSecTimeOriginal', 'SubSecTime_Original', str),
    ('Make', 'Make', str), ('Model', 'Model', str), ('LensModel', 'LensModel', str),
    ('ExifImageWidth', 'PixelXDimension', int),
    ('ExifImageHeight', 'PixelYDimension', int), ('Orientation', 'Orientation', int),
    ('ExposureTime', 'ExposureTime', float), ('FNumber', 'FNumber', float),
    ('ISO', 'ISOSpeedRatings', int), ('FocalLength', 'FocalLength', float),
    ('GPSHPositioningError', 'GPSHPositioningError', float),
    ('GPSDOP', 'GPSDOP', float), ('GPSMeasureMode', 'GPSMeasureMode', str),
    ('GPSImgDirectionRef', 'GPSImgDirectionRef', str),
    ('GPSDateStamp', 'GPSDateStamp', str), ('GPSTimeStamp', 'GPSTimeStamp', str),
)


def read_exif(photo: Path) -> dict[str, str]:
    """Read EXIF without pixel decoding or modifying the source photo.

    _codex-gpt-6.1-sol- on behalf of Matt Wilkie_
    """
    from osgeo import gdal

    with gdal.ExceptionMgr(), gdal.quiet_errors():
        try:
            with gdal.OpenEx(str(photo), gdal.OF_RASTER | gdal.OF_READONLY) as image:
                return {**(image.GetMetadata() or {}), **(image.GetMetadata('EXIF') or {})}
        except RuntimeError as exc:
            print(f'Warning: cannot read EXIF for {photo}: {exc}', file=sys.stderr)
            return {}


def attribute_exif(path: Path) -> None:
    """Add nullable EXIF attributes while preserving QGIS geometry and fields.

    Keep offsets, fractional-second digits and GPS time as text; numeric
    rationals are stored as numbers for filtering. Never infer absent tags.

    _codex-gpt-6.1-sol- on behalf of Matt Wilkie_
    """
    from osgeo import ogr

    types = {str: ogr.OFTString, int: ogr.OFTInteger64, float: ogr.OFTReal}
    with ogr.ExceptionMgr(), ogr.Open(str(path), update=1) as ds:
        if ds is None or ds.GetLayerCount() != 1:
            raise RuntimeError(f'Expected one photo layer in {path}')
        layer = ds.GetLayer(0)
        if layer.GetLayerDefn().GetFieldIndex('photo') < 0:
            raise RuntimeError(f'Missing photo attribute in {path}')
        layer.StartTransaction()
        try:
            for name, _, kind in EXIF_FIELDS:
                if layer.GetLayerDefn().GetFieldIndex(name) < 0:
                    layer.CreateField(ogr.FieldDefn(name, types[kind]))
            for feature in layer:
                photo = feature.GetField('photo')
                if not photo or not Path(photo).is_absolute():
                    raise RuntimeError(f'Expected absolute photo path, got {photo!r}')
                metadata = read_exif(Path(photo))
                for name, tag, kind in EXIF_FIELDS:
                    raw = metadata.get('EXIF_' + tag)
                    feature.SetFieldNull(name)
                    if raw is None or not raw.strip():
                        continue
                    try:
                        value = raw if kind is str else kind(
                            raw.strip().removeprefix('(').removesuffix(')'))
                        if kind is float and not math.isfinite(value):
                            continue
                        feature.SetField(name, value)
                    except (ValueError, OverflowError):
                        continue
                layer.SetFeature(feature)
            layer.CommitTransaction()
        except BaseException:
            layer.RollbackTransaction()
            raise


def style_sources(path: Path) -> None:
    """Embed a default QGIS categorized point style in the staging GeoPackage.

    _codex-gpt-6.1-sol- on behalf of Matt Wilkie_
    """
    with closing(sqlite3.connect(path)) as db, db:
        table, geometry = db.execute(
            'SELECT table_name, column_name FROM gpkg_geometry_columns'
        ).fetchone()
        quoted_table = '"' + table.replace('"', '""') + '"'
        folders = [row[0] for row in db.execute(
            f'SELECT DISTINCT source_folder FROM {quoted_table} ORDER BY source_folder'
        )]
        qml = ET.Element('qgis', version='3.40', styleCategories='Symbology')
        renderer = ET.SubElement(qml, 'renderer-v2', type='categorizedSymbol', attr='source_folder')
        categories = ET.SubElement(renderer, 'categories')
        symbols = ET.SubElement(renderer, 'symbols')
        for number, folder in enumerate(folders):
            symbol_id = str(number)
            ET.SubElement(categories, 'category', value=folder, label=folder,
                          symbol=symbol_id, render='true', type='string')
            symbol = ET.SubElement(symbols, 'symbol', name=symbol_id, type='marker', alpha='1')
            layer = ET.SubElement(symbol, 'layer', {'class': 'SimpleMarker', 'enabled': '1'})
            options = ET.SubElement(layer, 'Option', type='Map')
            properties = {
                'name': 'circle', 'size': '2', 'size_unit': 'MM',
                'color': ','.join(str(random.randrange(40, 231)) for _ in range(3)) + ',255',
                'outline_color': '50,50,50,255', 'outline_width': '0.2',
                'outline_width_unit': 'MM',
            }
            for name, value in properties.items():
                ET.SubElement(options, 'Option', name=name, value=value, type='QString')
        db.execute('''CREATE TABLE layer_styles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            f_table_catalog TEXT, f_table_schema TEXT, f_table_name TEXT,
            f_geometry_column TEXT, styleName TEXT, styleQML TEXT, styleSLD TEXT,
            useAsDefault BOOLEAN, description TEXT, owner TEXT, ui TEXT,
            update_time DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )''')
        db.execute('''INSERT INTO layer_styles
            (f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
             styleName, styleQML, useAsDefault, description, owner)
            VALUES ('', '', ?, ?, 'source_folder', ?, 1, 'Photos by source folder', '')''',
                   (table, geometry, ET.tostring(qml, encoding='unicode')))


def publish_index(source: Path, target: Path, *, overwrite: bool = False) -> None:
    """Publish atomically, replacing an existing index only when requested."""
    if overwrite:
        source.replace(target)
    elif os.name == 'nt':
        source.rename(target)
    else:
        os.link(source, target)
        source.unlink()


def index_folder(folder: Path, *, overwrite: bool = False) -> bool:
    """Skip existing indexes unless requested; publish only after a successful build."""
    folder = folder.resolve()
    output = folder / 'photo_locations.gpkg'
    if output.exists() and not overwrite:
        return False
    with tempfile.TemporaryDirectory(prefix='.photo-index-', dir=folder) as temporary:
        staging = Path(temporary) / output.name
        env = dict(os.environ, QT_QPA_PLATFORM='offscreen')
        subprocess.run(
            [shutil.which('qgis_process') or 'qgis_process', '--skip-loading-plugins', 'run', 'native:importphotos',
             '--', f'FOLDER={folder}', 'RECURSIVE=true', f'OUTPUT={staging}'],
            check=True, env=env,
        )
        if not staging.is_file():
            raise RuntimeError(f'QGIS did not produce an index for {folder}')
        attribute_sources(staging)
        attribute_exif(staging)
        style_sources(staging)
        publish_index(staging, output, overwrite=overwrite)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--overwrite', '--force', action='store_true',
                        help='Rebuild existing indexes; replace only after successful indexing')
    parser.add_argument('pattern', help="Directory path or immediate-subdirectory wildcard (e.g. '2024*' or '*')")
    args = parser.parse_args(argv)
    try:
        folders = match_folders(args.pattern, Path.cwd())
        if not folders:
            print(f'No directories match pattern: {args.pattern}')
            return 0
        for number, folder in enumerate(folders, 1):
            print(f'[{number}/{len(folders)}] Checking: {folder}', flush=True)
            indexed = index_folder(folder, overwrite=args.overwrite)
            print(f'{"Indexed" if indexed else "Skipping existing index"}: {folder}')
        print(f'Finished: checked {len(folders)} matching directories.')
        return 0
    except (OSError, RuntimeError, sqlite3.Error, subprocess.CalledProcessError) as exc:
        print(f'Photo indexing failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

