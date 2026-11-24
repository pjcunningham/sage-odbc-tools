import logging
from importlib.metadata import distribution
from pathlib import Path

from click.testing import CliRunner

from sageodbc import manager as manager_module
from sageodbc.manager import Manager


def test_templates_load_from_package_outside_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    manager = Manager(logging.getLogger('test.packaging'), 'dsn', 'user', 'password')

    template = manager.env.get_template('mysql-create.html')

    assert template.name == 'mysql-create.html'
    assert Path(manager.env.loader.searchpath[0]) == (
        Path(manager_module.__file__).parent / 'templates'
    )


def test_templates_load_from_pyinstaller_bundle(tmp_path, monkeypatch):
    bundled_templates = tmp_path / 'templates'
    bundled_templates.mkdir()
    (bundled_templates / 'mysql-create.html').write_text('bundled', encoding='utf-8')
    monkeypatch.setitem(manager_module.sys.__dict__, '_MEIPASS', str(tmp_path))

    manager = Manager(logging.getLogger('test.packaging.bundle'), 'dsn', 'user', 'password')

    assert manager.env.get_template('mysql-create.html').render() == 'bundled'


def test_console_script_entrypoint_reports_version():
    entrypoint = next(
        item for item in distribution('sage-odbc-tools').entry_points
        if item.name == 'sageodbc'
    )
    result = CliRunner().invoke(entrypoint.load(), ['--version'])

    assert result.exit_code == 0
    assert '0.1.0' in result.output
