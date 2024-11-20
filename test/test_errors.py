import duckdb
import pytest

from duckdb_appender.errors import (
    AppendAfterCloseError,
    AppenderDoubleCloseError,
    TableNotFoundError,
    ColumnCountError,
)
from duckdb_appender.appender import Appender


TEST_DB = "test/test.duckdb"


class TestErrors:

    def test_table_not_found_error(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        with pytest.raises(TableNotFoundError):
            appender = Appender(conn, "main", "wrong_table_name")

    def test_column_count_error(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        appender = Appender(conn, "main", "users")
        with pytest.raises(ColumnCountError):
            appender.append_row([1, "John", "john@email.com"])

    def test_close_appender_twice(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        appender = Appender(conn, "main", "users")
        appender.close()
        with pytest.raises(AppenderDoubleCloseError):
            appender.close()

    def test_append_row_after_close(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        appender = Appender(conn, "main", "users")
        appender.close()
        with pytest.raises(AppendAfterCloseError):
            appender.append_row([1, "John Doe"])
