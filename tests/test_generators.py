import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from sageodbc.manager import Manager
from sageodbc.models import Column, Table

TEMPLATE_DIRECTORY = Path(__file__).parents[1] / 'sageodbc' / 'templates'


def _manager():
    manager = Manager(logging.getLogger('test.generators'), 'dsn', 'user', 'password')
    manager.env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIRECTORY),
        autoescape=False,
    )
    return manager


def _column(name, data_type, type_name='varchar', size=40, remarks=None):
    return Column(
        None, None, 'SOURCE_TABLE', name, data_type, type_name, size, size,
        None, None, True, remarks,
    )


def test_query_to_sql_escapes_identifiers_and_values(tmp_path, monkeypatch):
    manager = _manager()
    connection = object()
    dataframe = pd.DataFrame(
        {
            'id': pd.Series([7, None], dtype='object'),
            'description': pd.Series(["O'Reilly \\ archive", None], dtype='object'),
        }
    )
    monkeypatch.setattr(manager, 'get_connection', lambda: connection)
    monkeypatch.setattr(
        'pandas.io.sql.read_sql',
        lambda query, actual_connection: dataframe,
    )
    output = tmp_path / 'insert.sql'

    manager.query_to_sql('select id, description from source', 'archive.records', str(output))

    sql = output.read_text(encoding='utf-8')
    assert 'insert into `archive`.`records` (`id`, `description`)' in sql
    assert "(7, 'O''Reilly \\\\ archive')" in sql
    assert '(NULL, NULL)' in sql


def test_query_to_sql_writes_valid_output_for_empty_results(tmp_path, monkeypatch):
    manager = _manager()
    monkeypatch.setattr(manager, 'get_connection', lambda: object())
    monkeypatch.setattr(
        'pandas.io.sql.read_sql',
        lambda query, connection: pd.DataFrame(columns=['id']),
    )
    output = tmp_path / 'empty.sql'

    manager.query_to_sql('select id from source where 1 = 0', 'target', str(output))

    sql = output.read_text(encoding='utf-8')
    assert 'values ;' not in sql
    assert 'No rows returned' in sql


def test_mysql_ddl_escapes_identifiers_and_comments(tmp_path, monkeypatch):
    manager = _manager()
    columns = [
        _column('SELECT', 1, size=20, remarks="O'Reilly & <script>"),
        _column('back`tick', 4, 'integer', remarks='identifier test'),
        _column('unknown', 9999, 'unsupported'),
    ]
    monkeypatch.setattr(manager, '_get_columns', lambda connection, table_name: columns)
    output = tmp_path / 'schema.sql'

    manager._internal_schema_to_mysql_ddl(object(), 'ORDER', str(output))

    sql = output.read_text(encoding='utf-8')
    assert 'create table if not exists `order_`' in sql.lower()
    assert '`select_`' in sql
    assert '`back``tick`' in sql
    assert '`unknown`' not in sql
    assert "COMMENT 'O''Reilly & <script>'" in sql
    assert 'COMMENT None' not in sql
    assert '&amp;' not in sql
    assert '&#' not in sql


def test_mysql_loader_matches_csv_escaping_and_quotes_paths(tmp_path, monkeypatch):
    manager = _manager()
    table = Table(None, None, 'ORDER', 'TABLE', '')
    columns = [
        _column('SELECT', 1),
        _column('ORDER_DATE', 9, 'datetime'),
    ]
    monkeypatch.setattr(manager, 'get_connection', lambda: object())
    excluded = Table(None, None, 'EXCLUDED', 'TABLE', '')
    monkeypatch.setattr(manager, '_get_tables', lambda connection: [table, excluded])
    monkeypatch.setattr(manager, '_get_columns', lambda connection, name: columns)
    output = tmp_path / 'load.sql'

    manager.generate_mysql_load_data(
        r"C:\data files\O'Brien", str(output), ['EXCLUDED']
    )

    sql = output.read_text(encoding='utf-8')
    assert "LOAD DATA INFILE 'C:/data files/O''Brien/order.csv'" in sql
    assert 'INTO TABLE `order_`' in sql
    assert 'FIELDS TERMINATED BY' in sql
    assert "ESCAPED BY '\"'" in sql
    assert "LINES TERMINATED BY '\\n'" in sql
    assert '`select_`' in sql
    assert "`order_date` = NULLIF(@sageodbc_2, '')" in sql
    assert '\\r\\n' not in sql
    assert 'excluded.csv' not in sql


def test_rest_schema_maps_types_and_keeps_code_comments_unescaped(tmp_path, monkeypatch):
    manager = _manager()
    columns = [_column('class', 4, 'integer', remarks='first line\nsecond <line>')]
    monkeypatch.setattr(manager, '_get_columns', lambda connection, table_name: columns)
    output = tmp_path / 'schema.py'

    manager._internal_dump_table_rest_schema(object(), 'ORDER', str(output))

    schema = output.read_text(encoding='utf-8')
    assert 'class_ = fields.Integer(as_string=True, dump_only=True)' in schema
    assert '# first line second <line>' in schema
    assert '&lt;line&gt;' not in schema


def test_json_net_schema_escapes_string_values(tmp_path, monkeypatch):
    manager = _manager()
    columns = [_column('name"field', 1, remarks='first line\nsecond line')]
    monkeypatch.setattr(manager, '_get_columns', lambda connection, table_name: columns)
    output = tmp_path / 'schema.cs'

    manager._internal_dump_table_json_net_schema(
        object(), 'CUSTOMER', 'Example.Schema', str(output)
    )

    schema = output.read_text(encoding='utf-8')
    assert r'JsonProperty("name\"field"' in schema
    assert 'public string Name_field;' in schema
    assert '// first line second line' in schema


def test_mysql_ddl_output_is_repeatable_with_a_fixed_timestamp(tmp_path, monkeypatch):
    class FixedDateTime:
        @staticmethod
        def now(tz):
            return datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    manager = _manager()
    columns = [_column('CUSTOMER_ID', 4, 'integer')]
    monkeypatch.setattr('sageodbc.generators.datetime', FixedDateTime)
    monkeypatch.setattr(manager, '_get_columns', lambda connection, table_name: columns)
    first = tmp_path / 'first.sql'
    second = tmp_path / 'second.sql'

    manager._internal_schema_to_mysql_ddl(object(), 'CUSTOMER', str(first))
    manager._internal_schema_to_mysql_ddl(object(), 'CUSTOMER', str(second))

    assert first.read_text(encoding='utf-8') == second.read_text(encoding='utf-8')
