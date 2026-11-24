from contextlib import closing

import pyodbc

from .models import Column, Table


class DatabaseConnectionError(RuntimeError):
    pass


class Database:
    def __init__(self, dsn: str, username: str, password: str):
        self.dsn = dsn
        self.password = password
        self.connection_string = f'DSN={dsn};UID={username};PWD={password}'
        self.connection = None

    def get_connection(self):
        if self.connection is not None:
            return self.connection

        try:
            self.connection = pyodbc.connect(self.connection_string)
        except Exception as ex:
            raise DatabaseConnectionError(
                f"Unable to connect to Sage ODBC DSN '{self.dsn}'. "
                'Verify that the DSN is configured and reachable.'
            ) from ex

        return self.connection

    def close(self):
        connection = self.connection
        self.connection = None
        if connection is not None:
            connection.close()

    def get_tables(self, connection):
        with closing(connection.cursor()) as cursor:
            return [Table.from_row(row) for row in cursor.tables()]

    def get_columns(self, connection, table_name):
        with closing(connection.cursor()) as cursor:
            return [Column.from_row(row) for row in cursor.columns(table=table_name)]
