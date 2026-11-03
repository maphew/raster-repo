"""Index geotagged photos recursively using QGIS, then attribute source folders.

_codex-gpt-6.1-sol- on behalf of Matt Wilkie_
"""
from __future__ import annotations

import argparse
import fnmatch
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


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


def publish_index(source: Path, target: Path) -> None:
    """Publish atomically without replacing an index created by another process."""
    if os.name == 'nt':
        source.rename(target)
    else:
        os.link(source, target)
        source.unlink()


def index_folder(folder: Path) -> bool:
    """Return False for an existing index; never change source photos."""
    folder = folder.resolve()
    output = folder / 'photo_locations.gpkg'
    if output.exists():
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
        publish_index(staging, output)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('pattern', help="Directory path or immediate-subdirectory wildcard (e.g. '2024*' or '*')")
    args = parser.parse_args(argv)
    try:
        folders = match_folders(args.pattern, Path.cwd())
        if not folders:
            print(f'No directories match pattern: {args.pattern}')
            return 0
        for number, folder in enumerate(folders, 1):
            print(f'[{number}/{len(folders)}] Checking: {folder}', flush=True)
            indexed = index_folder(folder)
            print(f'{"Indexed" if indexed else "Skipping existing index"}: {folder}')
        print(f'Finished: checked {len(folders)} matching directories.')
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f'Photo indexing failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

