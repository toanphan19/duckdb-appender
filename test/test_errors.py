import duckdb
import pytest

from pyappender.errors import AppendAfterCloseError, AppenderDoubleCloseError
from pyappender.pyappender import Appender


TEST_DB = "test/test.duckdb"


class TestErrors:

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
