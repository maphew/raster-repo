from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest
from osgeo import ogr, osr


@pytest.fixture
def indexer():
    path = Path(__file__).parents[1] / 'scripts' / 'index-geo-photos.py'
    spec = importlib.util.spec_from_file_location('index_geo_photos', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_photos(path, photos):
    with ogr.GetDriverByName('GPKG').CreateDataSource(str(path)) as ds:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        layer = ds.CreateLayer('photo_locations', srs, ogr.wkbPoint25D)
        layer.CreateField(ogr.FieldDefn('photo', ogr.OFTString))
        for photo in photos:
            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetField('photo', str(photo))
            feature.SetGeometry(ogr.CreateGeometryFromWkt('POINT Z (-135 61 123)'))
            layer.CreateFeature(feature)


def test_matching_literal_and_immediate_case_insensitive_pattern(indexer, tmp_path):
    literal = tmp_path / 'Trip[1]'
    literal.mkdir()
    (tmp_path / 'TRIP2').mkdir()
    (literal / 'TripNested').mkdir()
    assert indexer.match_folders(str(literal), tmp_path) == [literal]
    assert indexer.match_folders('trip*', tmp_path) == [tmp_path / 'TRIP2', literal]
    assert indexer.match_folders('absent*', tmp_path) == []


def test_attributes_preserve_geometry_and_distinguish_nested_paths(indexer, tmp_path):
    photos = [tmp_path / 'one' / 'Survey' / 'a.jpg',
              tmp_path / 'two' / 'Survey' / 'b.jpg', tmp_path / 'c.jpg']
    gpkg = tmp_path / 'photos.gpkg'
    make_photos(gpkg, photos)
    indexer.attribute_sources(gpkg)
    with ogr.Open(str(gpkg)) as ds:
        layer = ds.GetLayer(0)
        assert layer.GetSpatialRef().GetAuthorityCode(None) == '4326'
        rows = list(layer)
        assert [f.GetField('source_folder') for f in rows] == ['Survey', 'Survey', tmp_path.name]
        assert [f.GetField('source_path') for f in rows] == [str(p.parent) for p in photos]
        assert [f.GetField('photo') for f in rows] == list(map(str, photos))
        assert all(f.GetGeometryRef().GetPoint() == (-135, 61, 123) for f in rows)


def test_import_and_skip_existing(indexer, tmp_path, monkeypatch):
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        assert kwargs['check'] is True
        assert kwargs['env']['QT_QPA_PLATFORM'] == 'offscreen'
        assert f'FOLDER={tmp_path}' in command
        assert 'RECURSIVE=true' in command
        output = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        assert not (tmp_path / 'photo_locations.gpkg').exists()
        make_photos(output, [tmp_path / 'nested' / 'a.jpg'])
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    assert indexer.index_folder(tmp_path) is True
    output = tmp_path / 'photo_locations.gpkg'
    original = output.read_bytes()
    assert indexer.index_folder(tmp_path) is False
    assert output.read_bytes() == original
    assert len(calls) == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == ['photo_locations.gpkg']
    with sqlite3.connect(output) as db:
        assert db.execute('SELECT useAsDefault FROM layer_styles').fetchone() == (1,)


@pytest.mark.parametrize('folders', [[], ['Survey', 'Survey', 'Rock & "Ice"', "O'Brien"]])
def test_default_style_categories_and_symbols(indexer, tmp_path, folders):
    gpkg = tmp_path / 'photos.gpkg'
    make_photos(gpkg, [tmp_path / folder / f'{i}.jpg' for i, folder in enumerate(folders)])
    indexer.attribute_sources(gpkg)
    indexer.style_sources(gpkg)
    with sqlite3.connect(gpkg) as db:
        table, geometry, default, qml = db.execute(
            'SELECT f_table_name, f_geometry_column, useAsDefault, styleQML FROM layer_styles'
        ).fetchone()
        assert (table, geometry, default) == ('photo_locations', 'geom', 1)
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        updated = db.execute('SELECT update_time FROM layer_styles').fetchone()[0]
        assert 'T' in updated and updated.endswith('Z')
    renderer = ET.fromstring(qml).find('renderer-v2')
    assert renderer.get('type') == 'categorizedSymbol'
    assert renderer.get('attr') == 'source_folder'
    categories = renderer.findall('categories/category')
    assert [c.get('value') for c in categories] == sorted(set(folders))
    symbols = {s.get('name'): s for s in renderer.findall('symbols/symbol')}
    for category in categories:
        assert category.get('label') == category.get('value')
        assert category.get('render') == 'true'
        symbol = symbols[category.get('symbol')]
        assert symbol.get('type') == 'marker'
        assert symbol.find('layer').get('class') == 'SimpleMarker'
        assert symbol.find(".//Option[@name='color']") is not None
    with ogr.Open(str(gpkg)) as ds:
        layer = ds.GetLayerByName('photo_locations')
        assert layer.GetFeatureCount() == len(folders)
        assert all(f.GetGeometryRef().GetPoint() == (-135, 61, 123) for f in layer)


@pytest.mark.parametrize('failure', ['process', 'attributes', 'missing_output'])
def test_failure_does_not_publish_and_cleans_temporary_files(indexer, tmp_path, monkeypatch, failure):
    def run(command, **kwargs):
        output = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        if failure == 'missing_output':
            return
        output.write_bytes(b'incomplete')
        if failure == 'process':
            raise subprocess.CalledProcessError(1, command)
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    with pytest.raises((subprocess.CalledProcessError, RuntimeError)):
        indexer.index_folder(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_empty_layer_gets_attributes(indexer, tmp_path):
    gpkg = tmp_path / 'empty.gpkg'
    make_photos(gpkg, [])
    indexer.attribute_sources(gpkg)
    with ogr.Open(str(gpkg)) as ds:
        layer = ds.GetLayer(0)
        assert layer.GetFeatureCount() == 0
        assert layer.GetLayerDefn().GetFieldIndex('source_folder') >= 0
        assert layer.GetLayerDefn().GetFieldIndex('source_path') >= 0


def test_publish_never_overwrites_existing(indexer, tmp_path):
    source, target = tmp_path / 'new.gpkg', tmp_path / 'photo_locations.gpkg'
    source.write_bytes(b'new')
    target.write_bytes(b'original')
    with pytest.raises(FileExistsError):
        indexer.publish_index(source, target)
    assert target.read_bytes() == b'original'


def test_overwrite_existing_index(indexer, tmp_path, monkeypatch):
    output = tmp_path / 'photo_locations.gpkg'
    output.write_bytes(b'original')
    def run(command, **kwargs):
        assert output.read_bytes() == b'original'
        staging = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        make_photos(staging, [tmp_path / 'new' / 'a.jpg'])
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    assert indexer.index_folder(tmp_path, overwrite=True) is True
    with ogr.Open(str(output)) as ds:
        feature = ds.GetLayer(0).GetNextFeature()
        assert feature.GetField('source_folder') == 'new'
    assert sorted(p.name for p in tmp_path.iterdir()) == ['photo_locations.gpkg']


@pytest.mark.parametrize('failure', ['process', 'attributes', 'missing_output'])
def test_overwrite_failure_preserves_existing_index(indexer, tmp_path, monkeypatch, failure):
    output = tmp_path / 'photo_locations.gpkg'
    output.write_bytes(b'original')
    def run(command, **kwargs):
        staging = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        if failure == 'missing_output':
            return
        staging.write_bytes(b'incomplete')
        if failure == 'process':
            raise subprocess.CalledProcessError(1, command)
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    with pytest.raises((subprocess.CalledProcessError, RuntimeError)):
        indexer.index_folder(tmp_path, overwrite=True)
    assert output.read_bytes() == b'original'
    assert sorted(p.name for p in tmp_path.iterdir()) == ['photo_locations.gpkg']


@pytest.mark.parametrize('flag', ['--overwrite', '--force'])
def test_cli_overwrite_aliases(indexer, tmp_path, monkeypatch, flag):
    calls = []
    def index(folder, *, overwrite=False):
        calls.append((folder, overwrite))
        return True
    monkeypatch.setattr(indexer, 'index_folder', index)
    assert indexer.main([flag, str(tmp_path)]) == 0
    assert calls == [(tmp_path, True)]


def test_cli_help_no_matches_and_failure(indexer, tmp_path, monkeypatch, capsys):
    with pytest.raises(SystemExit) as exc:
        indexer.main(['--help'])
    assert exc.value.code == 0
    monkeypatch.chdir(tmp_path)
    assert indexer.main(['nothing*']) == 0
    assert 'No directories match' in capsys.readouterr().out
    def fail(folder, *, overwrite=False):
        raise FileNotFoundError('qgis_process not found')
    monkeypatch.setattr(indexer, 'index_folder', fail)
    assert indexer.main([str(tmp_path)]) == 1
    assert 'qgis_process not found' in capsys.readouterr().err



def test_resolves_qgis_command_shims(indexer, tmp_path, monkeypatch):
    shim = str(tmp_path / 'QGIS bin' / 'qgis_process.cmd')
    import shutil
    monkeypatch.setattr(shutil, 'which', lambda name: shim)
    def run(command, **kwargs):
        assert command[0] == shim
        output = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        make_photos(output, [])
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    assert indexer.index_folder(tmp_path)


def test_additional_exif_fields_from_real_jpeg(indexer, tmp_path):
    from osgeo import gdal

    photo = tmp_path / 'a.jpg'
    metadata = {
        'EXIF_OffsetTimeOriginal': '-07:00', 'EXIF_SubSecTime_Original': '001230',
        'EXIF_Make': 'Example', 'EXIF_Model': 'Survey Camera', 'EXIF_LensModel': 'Wide 24',
        'EXIF_PixelXDimension': '4000', 'EXIF_PixelYDimension': '3000',
        'EXIF_Orientation': '6', 'EXIF_ExposureTime': '(0.008)', 'EXIF_FNumber': '(2.8)',
        'EXIF_ISOSpeedRatings': '200', 'EXIF_FocalLength': '(24)',
        'EXIF_GPSHPositioningError': '(3.5)', 'EXIF_GPSDOP': '(1.2)',
        'EXIF_GPSMeasureMode': '3', 'EXIF_GPSImgDirectionRef': 'M',
        'EXIF_GPSDateStamp': '2026:11:04', 'EXIF_GPSTimeStamp': '(12) (34) (56.125)',
    }
    with gdal.GetDriverByName('MEM').Create('', 2, 2, 3) as image:
        image.SetMetadata(metadata)
        with gdal.GetDriverByName('JPEG').CreateCopy(str(photo), image):
            pass
    original = photo.read_bytes()
    gpkg = tmp_path / 'photos.gpkg'
    make_photos(gpkg, [photo])
    indexer.attribute_exif(gpkg)
    expected = {
        'OffsetTimeOriginal': '-07:00', 'SubSecTimeOriginal': '001230',
        'Make': 'Example', 'Model': 'Survey Camera', 'LensModel': 'Wide 24',
        'ExifImageWidth': 4000, 'ExifImageHeight': 3000, 'Orientation': 6,
        'ExposureTime': 0.008, 'FNumber': 2.8, 'ISO': 200, 'FocalLength': 24,
        'GPSHPositioningError': 3.5, 'GPSDOP': 1.2, 'GPSMeasureMode': '3',
        'GPSImgDirectionRef': 'M', 'GPSDateStamp': '2026:11:04',
        'GPSTimeStamp': '(12) (34) (56.125)',
    }
    with ogr.Open(str(gpkg)) as ds:
        feature = ds.GetLayer(0).GetNextFeature()
        for name, value in expected.items():
            assert feature.GetField(name) == value, name
        assert feature.GetGeometryRef().GetPoint() == (-135, 61, 123)
    assert photo.read_bytes() == original


def test_missing_and_invalid_exif_are_null(indexer, tmp_path, monkeypatch):
    gpkg = tmp_path / 'photos.gpkg'
    make_photos(gpkg, [tmp_path / 'a.jpg', tmp_path / 'b.jpg'])
    monkeypatch.setattr(indexer, 'read_exif', lambda path: {
        'EXIF_FNumber': '(not a number)', 'EXIF_ExposureTime': '(nan)',
        'EXIF_ISOSpeedRatings': '100 200', 'EXIF_Orientation': '1.5',
        'EXIF_GPSHPositioningError': '(0)',
    } if path.name == 'a.jpg' else {})
    indexer.attribute_exif(gpkg)
    indexer.attribute_exif(gpkg)
    with ogr.Open(str(gpkg)) as ds:
        rows = list(ds.GetLayer(0))
        for name in ['OffsetTimeOriginal', 'Model', 'GPSImgDirectionRef',
                     'FNumber', 'ExposureTime', 'ISO', 'Orientation']:
            assert all(row.GetField(name) is None for row in rows)
        assert rows[0].GetField('GPSHPositioningError') == 0
        assert rows[1].GetField('GPSHPositioningError') is None


def test_index_enriches_exif_before_publication(indexer, tmp_path, monkeypatch):
    def run(command, **kwargs):
        output = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        make_photos(output, [tmp_path / 'a.jpg'])
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    monkeypatch.setattr(indexer, 'read_exif', lambda path: {'EXIF_Model': 'Camera'})
    indexer.index_folder(tmp_path)
    with ogr.Open(str(tmp_path / 'photo_locations.gpkg')) as ds:
        assert ds.GetLayer(0).GetNextFeature().GetField('Model') == 'Camera'



def test_unreadable_photo_warns_and_returns_no_metadata(indexer, tmp_path, capsys):
    assert indexer.read_exif(tmp_path / 'missing.jpg') == {}
    assert 'cannot read EXIF' in capsys.readouterr().err


def test_exif_failure_preserves_existing_index(indexer, tmp_path, monkeypatch):
    output = tmp_path / 'photo_locations.gpkg'
    output.write_bytes(b'original')
    def run(command, **kwargs):
        staging = Path(next(arg.removeprefix('OUTPUT=') for arg in command if arg.startswith('OUTPUT=')))
        make_photos(staging, [tmp_path / 'a.jpg'])
    def fail(path):
        raise RuntimeError('EXIF attribution failed')
    monkeypatch.setattr(indexer.subprocess, 'run', run)
    monkeypatch.setattr(indexer, 'attribute_exif', fail)
    with pytest.raises(RuntimeError, match='EXIF attribution failed'):
        indexer.index_folder(tmp_path, overwrite=True)
    assert output.read_bytes() == b'original'
    assert sorted(p.name for p in tmp_path.iterdir()) == ['photo_locations.gpkg']


def test_empty_layer_gets_exif_schema(indexer, tmp_path):
    gpkg = tmp_path / 'empty.gpkg'
    make_photos(gpkg, [])
    indexer.attribute_exif(gpkg)
    with ogr.Open(str(gpkg)) as ds:
        layer = ds.GetLayer(0)
        assert layer.GetFeatureCount() == 0
        assert all(layer.GetLayerDefn().GetFieldIndex(name) >= 0
                   for name, _, _ in indexer.EXIF_FIELDS)
