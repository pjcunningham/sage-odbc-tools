import json
import logging
import warnings
from types import SimpleNamespace
from xml.etree import ElementTree

import pandas as pd
import pytest

from sageodbc import exports
from sageodbc.manager import Manager
from sageodbc.models import Column, OutputFormatEnum, Table


def _manager(monkeypatch, dataframe):
    manager = Manager(logging.getLogger('test.exports'), 'dsn', 'user', 'password')
    connection = object()
    monkeypatch.setattr(manager, 'get_connection', lambda: connection)
    calls = []

    def read_sql(query, actual_connection, **kwargs):
        calls.append((query, actual_connection, kwargs))
        return dataframe

    monkeypatch.setattr('pandas.io.sql.read_sql', read_sql)
    return manager, connection, calls


@pytest.mark.parametrize('output_format', list(OutputFormatEnum))
def test_query_exports_dataframe_in_each_supported_format(tmp_path, monkeypatch, output_format):
    dataframe = pd.DataFrame({'name': ['Acme'], 'amount': [12]})
    manager, connection, calls = _manager(monkeypatch, dataframe)
    output = tmp_path / f'query.{output_format.value}'

    query = 'select name, amount from customers'
    manager.query(query, str(output), output_format)

    assert output.exists()
    assert calls == [(query, connection, {})]
    if output_format == OutputFormatEnum.csv:
        assert output.read_text(encoding='utf-8') == '"name","amount"\n"Acme",12\n'
    elif output_format == OutputFormatEnum.json:
        assert json.loads(output.read_text(encoding='utf-8'))['data'][0] == {
            'name': 'Acme',
            'amount': 12,
        }
    elif output_format == OutputFormatEnum.xlsx:
        assert output.read_bytes().startswith(b'PK')
    else:
        assert ElementTree.fromstring(output.read_text(encoding='utf-8')).tag == 'data'


def test_query_exports_empty_dataframe(tmp_path, monkeypatch):
    dataframe = pd.DataFrame(columns=['name'])
    manager, _, _ = _manager(monkeypatch, dataframe)
    output = tmp_path / 'empty.csv'

    manager.query('select name from customers where 1 = 0', str(output), OutputFormatEnum.csv)

    assert output.read_text(encoding='utf-8') == '"name"\n'


@pytest.mark.parametrize(
    ('include_deleted', 'expected_query'),
    [
        (False, 'select * from CUSTOMER where RECORD_DELETED = 0'),
        (True, 'select * from CUSTOMER'),
    ],
)
def test_dump_table_uses_expected_deleted_row_filter(
        tmp_path, monkeypatch, include_deleted, expected_query
):
    dataframe = pd.DataFrame({'NAME': ['Acme']})
    manager, connection, calls = _manager(monkeypatch, dataframe)
    table = Table(None, None, 'CUSTOMER', 'TABLE', '')
    column = Column(None, None, 'CUSTOMER', 'NAME', 12, 'varchar', 20, 20, None, None, False, None)
    monkeypatch.setattr(manager, '_get_tables', lambda actual_connection: [table])
    monkeypatch.setattr(manager, '_get_columns', lambda actual_connection, table_name: [column])
    output = tmp_path / 'customer.csv'

    manager.dump_table('CUSTOMER', str(output), OutputFormatEnum.csv, include_deleted)

    query, actual_connection, kwargs = calls[0]
    assert (query, actual_connection) == (expected_query, connection)
    assert kwargs['dtype'] == {'NAME': 'object'}
    assert output.exists()


def test_dump_table_rejects_unknown_table_before_query(tmp_path, monkeypatch):
    manager, _, calls = _manager(monkeypatch, pd.DataFrame())
    monkeypatch.setattr(manager, '_get_tables', lambda connection: [])

    with pytest.raises(ValueError, match='Table MISSING does not exist'):
        manager.dump_table('MISSING', str(tmp_path / 'missing.csv'), OutputFormatEnum.csv, False)

    assert calls == []


def test_dump_tables_uses_snake_case_bulk_filename(tmp_path, monkeypatch):
    manager, _, calls = _manager(monkeypatch, pd.DataFrame({'NAME': ['Acme']}))
    table = Table(None, None, 'CUSTOMER_DETAIL', 'TABLE', '')
    column = Column(None, None, 'CUSTOMER_DETAIL', 'NAME', 12, 'varchar', 20, 20, None, None, False, None)
    monkeypatch.setattr(manager, '_get_tables', lambda connection: [table])
    monkeypatch.setattr(manager, '_get_columns', lambda connection, table_name: [column])

    manager.dump_tables(str(tmp_path), OutputFormatEnum.csv, False)

    assert (tmp_path / 'customer_detail.csv').exists()
    assert calls[0][0] == 'select * from CUSTOMER_DETAIL where RECORD_DELETED = 0'


def test_query_preserves_underlying_read_error(tmp_path, monkeypatch):
    manager = Manager(logging.getLogger('test.exports'), 'dsn', 'user', 'password')
    connection = object()
    cause = RuntimeError('database query failed')
    monkeypatch.setattr(manager, 'get_connection', lambda: connection)

    def fail_read(*args, **kwargs):
        raise cause

    monkeypatch.setattr('pandas.io.sql.read_sql', fail_read)

    with pytest.raises(ValueError, match='Invalid query') as error:
        manager.query('select invalid', str(tmp_path / 'invalid.csv'), OutputFormatEnum.csv)

    assert error.value.__cause__ is cause


@pytest.mark.parametrize('read_function', ['query', 'table'])
def test_read_suppresses_only_pandas_connection_warning_and_restores_filters(
        monkeypatch, read_function
):
    logger = logging.getLogger('test.exports')

    def read_sql(*args, **kwargs):
        warnings.warn(
            'pandas only supports SQLAlchemy connectable connections',
            UserWarning,
        )
        warnings.warn('unrelated warning', RuntimeWarning)
        return SimpleNamespace(shape=(0, 0))

    monkeypatch.setattr('pandas.io.sql.read_sql', read_sql)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        filters_before = list(warnings.filters)

        if read_function == 'query':
            exports.read_query(object(), 'select 1', logger)
        else:
            exports.read_table(object(), 'CUSTOMER', False, {}, logger)

        assert warnings.filters == filters_before
        warnings.warn(
            'pandas only supports SQLAlchemy connectable connections',
            UserWarning,
        )

    assert [str(warning.message) for warning in caught] == [
        'unrelated warning',
        'pandas only supports SQLAlchemy connectable connections',
    ]
