from importlib.resources import files

from cmsrucio_import import __version__


def test_version():
    assert __version__ == '0.1.0'


def test_packaged_templates():
    templates = files("cmsrucio_import").joinpath("templates")
    assert {item.name for item in templates.iterdir()} == {
        "dataset-upload.yml",
        "dbs-dataset-import.yml",
        "file-upload.yml",
    }
