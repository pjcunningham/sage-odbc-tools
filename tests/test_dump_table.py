# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from click.testing import CliRunner
from sageodbc.cli import cli


class FakeCursor:
    def __init__(self):
        self.closed = False

    def tables(self):
        return []

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_dump_table_unknown(tmp_path, monkeypatch):
    connection = FakeConnection()
    connection.closed = False
    monkeypatch.setattr('sageodbc.database.pyodbc.connect', lambda _: connection)
    env_file = tmp_path / '.env'
    env_file.write_text(
        'SAGE_ODBC_DSN=test\nSAGE_USERNAME=test\nSAGE_PASSWORD=secret\n',
        encoding='utf-8',
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            '--env-file', str(env_file), 'dump-table', 'MADUPTABLE',
            '--output', str(tmp_path / 'out.csv'), '--format', 'csv',
        ],
    )

    assert result.exit_code != 0
    assert 'Table MADUPTABLE does not exist.' in result.output
    assert 'secret' not in result.output
    assert connection.cursor_instance.closed
    assert connection.closed
