import pytest

from sageodbc.database import Database, DatabaseConnectionError


class FakeCursor:
    def __init__(self, rows=None, failure=None):
        self.rows = rows or []
        self.failure = failure
        self.closed = False

    def tables(self):
        if self.failure:
            raise self.failure
        return self.rows

    def columns(self, table):
        if self.failure:
            raise self.failure
        return self.rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor=None):
        self.cursor_instance = cursor or FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_get_connection_caches_connection(monkeypatch):
    connection = FakeConnection()
    calls = []
    monkeypatch.setattr('sageodbc.database.pyodbc.connect', lambda connection_string: calls.append(connection_string) or connection)
    database = Database('sage', 'user', 'secret')

    assert database.get_connection() is connection
    assert database.get_connection() is connection
    assert calls == ['DSN=sage;UID=user;PWD=secret']


def test_get_connection_wraps_and_preserves_driver_error(monkeypatch):
    driver_error = OSError('driver unavailable')
    monkeypatch.setattr('sageodbc.database.pyodbc.connect', lambda _: (_ for _ in ()).throw(driver_error))
    database = Database('sage', 'user', 'secret')

    with pytest.raises(DatabaseConnectionError, match='Unable to connect to Sage ODBC DSN') as error:
        database.get_connection()

    assert error.value.__cause__ is driver_error


def test_close_releases_cached_connection():
    connection = FakeConnection()
    database = Database('sage', 'user', 'secret')
    database.connection = connection

    database.close()

    assert connection.closed
    assert database.connection is None


def test_metadata_cursor_closes_when_read_fails():
    failure = RuntimeError('metadata unavailable')
    cursor = FakeCursor(failure=failure)
    database = Database('sage', 'user', 'secret')

    with pytest.raises(RuntimeError, match='metadata unavailable'):
        database.get_tables(FakeConnection(cursor))

    assert cursor.closed
