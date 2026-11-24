from pathlib import Path

from click.testing import CliRunner

from sageodbc.cli import cli
from sageodbc.models import OutputFormatEnum
from sageodbc.manager import Manager


def _env_file(tmp_path):
    env_file = tmp_path / '.env'
    env_file.write_text(
        'SAGE_ODBC_DSN=test\nSAGE_USERNAME=test\nSAGE_PASSWORD=secret\n',
        encoding='utf-8',
    )
    return env_file


def test_dump_table_writes_text_to_stdout_by_default(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)

    def dump_table(self, table_name, output_filename, output_format, include_deleted):
        assert table_name == 'CUSTOMERS'
        assert output_format is OutputFormatEnum.csv
        Path(output_filename).write_bytes(b'name,value\r\nA,1\r\n')

    monkeypatch.setattr(Manager, 'dump_table', dump_table)
    result = CliRunner().invoke(
        cli,
        ['--env-file', str(env_file), 'dump-table', 'CUSTOMERS'],
    )

    assert result.exit_code == 0
    assert result.output == 'name,value\r\nA,1\r\n'


def test_dump_table_writes_to_output_path(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    output_path = tmp_path / 'customers.csv'

    def dump_table(self, table_name, output_filename, output_format, include_deleted):
        Path(output_filename).write_text('name,value\nA,1\n', encoding='utf-8')

    monkeypatch.setattr(Manager, 'dump_table', dump_table)
    result = CliRunner().invoke(
        cli,
        [
            '--env-file', str(env_file), 'dump-table', 'CUSTOMERS',
            '--format', 'CSV', '--output', str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.read_text(encoding='utf-8') == 'name,value\nA,1\n'
    assert result.output == ''


def test_dump_table_rejects_stdout_for_xlsx(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    monkeypatch.setattr(
        Manager,
        'dump_table',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('must not export')),
    )
    result = CliRunner().invoke(
        cli,
        ['--env-file', str(env_file), 'dump-table', 'CUSTOMERS', '--format', 'xlsx'],
    )

    assert result.exit_code != 0
    assert 'XLSX output requires a file path' in result.output


def test_query_from_file_preserves_sql_newlines(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    query_file = tmp_path / 'query.sql'
    query_text = 'SELECT \'one\'\r\n-- keep comment\r\nFROM CUSTOMERS'
    query_file.write_bytes(query_text.encode('utf-8'))
    received = []

    def query(self, sql, output_filename, output_format):
        received.append((sql, output_format))
        Path(output_filename).write_bytes(b'ok\n')

    monkeypatch.setattr(Manager, 'query', query)
    result = CliRunner().invoke(
        cli,
        ['--env-file', str(env_file), 'query-from-file', '--input', str(query_file)],
    )

    assert result.exit_code == 0
    assert result.output == 'ok\n'
    assert received == [(query_text, OutputFormatEnum.csv)]


def test_query_from_file_reads_sql_from_stdin(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    query_text = 'SELECT 1\n-- stdin comment\n'
    received = []

    def query(self, sql, output_filename, output_format):
        received.append(sql)
        Path(output_filename).write_bytes(b'ok\n')

    monkeypatch.setattr(Manager, 'query', query)
    result = CliRunner().invoke(
        cli,
        ['--env-file', str(env_file), 'query-from-file', '--input', '-'],
        input=query_text,
    )

    assert result.exit_code == 0
    assert received == [query_text]
    assert result.output == 'ok\n'


def test_bulk_output_directory_is_a_required_existing_option(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    calls = []
    monkeypatch.setattr(
        Manager,
        'dump_tables',
        lambda self, output_directory, output_format, include_deleted: calls.append(
            (output_directory, output_format, include_deleted)
        ),
    )
    result = CliRunner().invoke(
        cli,
        [
            '--env-file', str(env_file), 'dump-tables',
            '--output-directory', str(tmp_path), '--format', 'json',
        ],
    )

    assert result.exit_code == 0
    assert calls == [(str(tmp_path), OutputFormatEnum.json, False)]

    missing_directory = tmp_path / 'missing'
    invalid_result = CliRunner().invoke(
        cli,
        [
            '--env-file', str(env_file), 'dump-tables',
            '--output-directory', str(missing_directory),
        ],
    )
    assert invalid_result.exit_code != 0
    assert 'does not exist' in invalid_result.output
