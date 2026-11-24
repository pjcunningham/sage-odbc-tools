from contextlib import contextmanager
from importlib import import_module
import logging
from pathlib import Path

import click
from click.testing import CliRunner
from jinja2 import Environment, FileSystemLoader

from sageodbc.cli import cli
from sageodbc.generators import generate_mysql_load_data
from sageodbc.manager import Manager
from sageodbc.models import OutputFormatEnum, Table
from sageodbc.schema_diff import compare_schema_paths

TEMPLATE_DIRECTORY = Path(__file__).parents[1] / 'sageodbc' / 'templates'
cli_module = import_module('sageodbc.cli')


def _env_file(tmp_path):
    env_file = tmp_path / '.env'
    env_file.write_text(
        'SAGE_ODBC_DSN=test\nSAGE_USERNAME=test\nSAGE_PASSWORD=secret\n',
        encoding='utf-8',
    )
    return env_file


def _recording_progress(state):
    @contextmanager
    def progress(items, length, label):
        state['length'] = length
        state['label'] = label
        state['closed'] = False
        try:
            yield iter(items)
        finally:
            state['closed'] = True

    return progress


def test_bulk_commands_offer_optional_progress():
    runner = CliRunner()
    commands = (
        'dump-tables',
        'dump-table-counts',
        'dump-table-schemas',
        'dump-table-rest-schemas',
        'dump-table-json-net-schemas',
        'schemas-to-mysql-ddl',
        'generate-mysql-load-data',
        'compare-schemas',
    )

    for command in commands:
        result = runner.invoke(cli, [command, '--help'])
        assert result.exit_code == 0
        assert '--progress' in result.output

    assert '--progress' not in runner.invoke(cli, ['dump-table', '--help']).output
    assert '--progress' not in runner.invoke(cli, ['dump-table-names', '--help']).output


def test_progress_is_opt_in_and_keeps_stdout_separate(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path)
    input_directory = tmp_path / 'input'
    input_directory.mkdir()
    calls = []

    class FakeManager:
        def __init__(self, *args):
            pass

        def close(self):
            pass

        @staticmethod
        def generate_mysql_load_data(_input_directory, output_filename, _exclude_tables, **kwargs):
            calls.append(kwargs)
            if 'progress' in kwargs:
                with kwargs['progress'](
                    ['CUSTOMERS'], length=1, label='Generating test data'
                ) as items:
                    assert list(items) == ['CUSTOMERS']
            Path(output_filename).write_text('LOAD DATA;\n', encoding='utf-8')

    @contextmanager
    def visible_progress(items, length, label):
        click.echo(f'{label}: {length}', err=True)
        yield iter(items)

    monkeypatch.setattr('sageodbc.cli.Manager', FakeManager)
    monkeypatch.setattr('click.progressbar', visible_progress)
    runner = CliRunner()
    base_args = [
        '--env-file', str(env_file), 'generate-mysql-load-data',
        '--input-directory', str(input_directory),
    ]

    enabled = runner.invoke(cli, [*base_args, '--progress'])
    disabled = runner.invoke(cli, base_args)

    assert enabled.exit_code == 0
    assert enabled.stdout.replace('\r\n', '\n') == 'LOAD DATA;\n'
    assert 'Generating test data: 1' in enabled.stderr
    assert disabled.exit_code == 0
    assert disabled.stdout.replace('\r\n', '\n') == 'LOAD DATA;\n'
    assert disabled.stderr == ''
    assert 'progress' not in calls[1]
    assert calls[0]['progress'] is visible_progress


def test_manager_progress_counts_tables_and_closes_after_error():
    manager = Manager.__new__(Manager)
    tables = [
        Table('catalog', 'schema', 'CUSTOMERS', 'TABLE', ''),
        Table('catalog', 'schema', 'ORDERS', 'TABLE', ''),
    ]
    manager.get_connection = lambda: object()
    manager._get_tables = lambda connection: tables
    processed = []
    manager._internal_dump_table = lambda connection, name, *args: processed.append(name)
    state = {}

    manager.dump_tables('output', OutputFormatEnum.csv, False, progress=_recording_progress(state))

    assert processed == ['CUSTOMERS', 'ORDERS']
    assert state == {'length': 2, 'label': 'Dumping tables', 'closed': True}

    manager._internal_dump_table = lambda *args: (_ for _ in ()).throw(RuntimeError('export failed'))
    state = {}
    try:
        manager.dump_tables('output', OutputFormatEnum.csv, False, progress=_recording_progress(state))
    except RuntimeError as error:
        assert str(error) == 'export failed'
    else:
        raise AssertionError('The export failure should propagate.')

    assert state['closed'] is True


def test_empty_table_list_uses_zero_length_progress():
    manager = Manager.__new__(Manager)
    manager.get_connection = lambda: object()
    manager._get_tables = lambda connection: []
    state = {}

    manager.dump_tables('output', OutputFormatEnum.csv, False, progress=_recording_progress(state))

    assert state == {'length': 0, 'label': 'Dumping tables', 'closed': True}


def test_mysql_load_progress_excludes_skipped_tables(tmp_path):
    tables = [
        Table('catalog', 'schema', 'CUSTOMERS', 'TABLE', ''),
        Table('catalog', 'schema', 'ORDERS', 'TABLE', ''),
    ]
    environment = Environment(loader=FileSystemLoader(TEMPLATE_DIRECTORY), autoescape=False)
    output = tmp_path / 'load.sql'
    state = {}

    generate_mysql_load_data(
        tables,
        lambda table_name: [],
        str(tmp_path),
        str(output),
        ['ORDERS'],
        environment,
        logging.getLogger('test.progress'),
        progress=_recording_progress(state),
    )

    assert state == {'length': 1, 'label': 'Generating MySQL load data', 'closed': True}
    sql = output.read_text(encoding='utf-8')
    assert 'customers' in sql.lower()
    assert 'orders' not in sql.lower()


def test_schema_comparison_progress_counts_common_files(tmp_path):
    left = tmp_path / 'left'
    right = tmp_path / 'right'
    left.mkdir()
    right.mkdir()
    (left / 'customers.txt').write_text('before\n', encoding='utf-8')
    (right / 'customers.txt').write_text('after\n', encoding='utf-8')
    (left / 'orders.txt').write_text('same\n', encoding='utf-8')
    (right / 'orders.txt').write_text('same\n', encoding='utf-8')
    (left / 'unmatched.txt').write_text('ignored\n', encoding='utf-8')
    state = {}

    differences = compare_schema_paths(left, right, progress=_recording_progress(state))

    assert state == {'length': 2, 'label': 'Comparing schema files', 'closed': True}
    assert differences['left_file'].tolist() == ['customers.txt']
