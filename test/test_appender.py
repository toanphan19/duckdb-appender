import datetime
import decimal
import uuid
import duckdb
import pytest
import pytz

from appender import Appender
from appender.appender import (
    SQLiteBuffer,
    TableSchema,
)


TEST_DB = "test/test.duckdb"


class TestSQLiteBuffer:
    def test_init(self):
        buffer = SQLiteBuffer(
            "users",
            TableSchema(
                table_name="users",
                column_names=["id", "name"],
                column_types=["INTEGER", "VARCHAR"],
            ),
        )

        rows = buffer._conn.execute("PRAGMA table_info(users)").fetchall()
        assert rows == [
            (0, "id", "INTEGER", 0, None, 0),
            (1, "name", "VARCHAR", 0, None, 0),
        ]

    def test_append_row(self):
        buffer = SQLiteBuffer(
            "users",
            TableSchema(
                table_name="users",
                column_names=["id", "name"],
                column_types=["INTEGER", "VARCHAR"],
            ),
        )
        buffer.append_row([1, "John Doe"])
        buffer.append_row([2, "Maria"])

        assert buffer._conn.execute("SELECT count(*) FROM users").fetchone()[0] == 2
        assert buffer._conn.execute("SELECT * FROM users").fetchall() == [
            (1, "John Doe"),
            (2, "Maria"),
        ]
        assert buffer.row_count == 2


class TestAppender:

    def test_get_duckdb_table_schema(self):
        # Set up the source duckdb table
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        appender = Appender(conn, "main", "users")
        table_schema = appender._get_duckdb_table_schema()
        appender.close()

        assert table_schema == TableSchema(
            table_name="users",
            column_names=["id", "name"],
            column_types=["INTEGER", "VARCHAR"],
        )

    def test_append_row(self):
        # Set up the source duckdb table
        conn = duckdb.connect()
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
        conn.commit()

        # Basic test append
        appender = Appender(conn, "main", "users")
        appender.append_row([1, "John Doe"])
        appender.append_row([2, "Maria"])
        appender.close()
        assert conn.execute("SELECT * FROM users").fetchall() == [
            (1, "John Doe"),
            (2, "Maria"),
        ]

        # Test append a bunch of rows
        appender = Appender(conn, "main", "users")
        for i in range(3, 1000):
            appender.append_row([i, f"User {i}"])
        appender.close()
        assert conn.execute("SELECT count(*) FROM users").fetchone() == (999,)

    def test_append_row_basic_number_types(self):
        conn = duckdb.connect()

        # Create a table to append to
        conn.execute(
            """
            CREATE TABLE test (
                i1 TINYINT, 
                i2 SMALLINT,
                i3 INTEGER,
                i4 BIGINT,
                f FLOAT,
                d DOUBLE,
            );"""
        )
        conn.commit()

        appender = Appender(conn, "main", "test")
        for _ in range(1000):
            appender.append_row([1, 1, 1, 1, 1.0625, 1.0625])
        appender.close()
        assert (
            conn.execute(
                """SELECT 
                        sum(i1),
                        sum(i2), 
                        sum(i3), 
                        sum(i4), 
                        sum(f), 
                        sum(d)
                    FROM test;
                """
            ).fetchone()
            == (1000, 1000, 1000, 1000, 1062.5, 1062.5)
        )

    def test_append_row_decimal(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (d DECIMAL(4, 3));")
        conn.commit()

        appender = Appender(conn, "main", "test")
        appender.append_row([1.11])
        appender.append_row([1.22])
        appender.close()
        assert conn.execute("SELECT sum(d) FROM test").fetchone() == (
            decimal.Decimal("2.33"),
        )

    def test_append_row_timestamp(self):
        conn = duckdb.connect()
        conn.execute(
            """
            CREATE TABLE test (
                t1 TIMESTAMP,
                t2 TIMESTAMPTZ
            );"""
        )
        conn.commit()

        appender = Appender(conn, "main", "test")

        t = datetime.datetime.now(pytz.timezone("America/Los_Angeles"))
        appender.append_row([t, t])
        appender.close()

        row = conn.execute("SELECT * FROM test").fetchone()
        assert row is not None
        assert row[0].replace(tzinfo=datetime.timezone.utc) == t
        assert row[1] == t

    def test_append_row_uuid(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (id UUID);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        u = uuid.uuid4()
        appender.append_row([u])
        appender.close()
        assert conn.execute("SELECT * FROM test").fetchone() == (u,)

    def test_append_row_blob(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (data BLOB);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        appender.append_row([b"hello world"])
        appender.close()

        assert conn.execute("SELECT * FROM test").fetchone() == (b"hello world",)

    def test_append_row_list(self):
        """List type in DuckDB is similar to Postgres' ARRAY type.
        Ref: https://duckdb.org/docs/sql/data_types/list
        """
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (string_list VARCHAR[]);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        string_array = ["a1", "b2", "ccc"]
        appender.append_row([string_array])
        appender.close()
        assert conn.execute("SELECT * FROM test").fetchone() == (string_array,)

        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (int_list INTEGER[]);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        appender.append_row([[1, 2, 3]])
        appender.close()
        assert conn.execute("SELECT * FROM test").fetchone() == ([1, 2, 3],)

    def test_append_row_array(self):
        """Array type is usually used for vectors/embeddings.
        Ref: https://duckdb.org/docs/sql/data_types/array
        """
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (string_array VARCHAR[3]);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        string_array = ["a1", "b2", "ccc"]
        appender.append_row([string_array])
        appender.close()
        assert conn.execute("SELECT * FROM test").fetchone() == (tuple(string_array),)

    def test_auto_flush(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i INTEGER);")
        conn.commit()

        appender = Appender(conn, "main", "test", 2000)
        for _ in range(4500):
            appender.append_row([1])
        assert conn.execute("SELECT count(*) FROM test").fetchone() == (4000,)


class TestAppenderDifferentUseCases:

    def test_append_row(self):
        """Should support tuples, arrays, and dict.values() as the input parameter."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i INTEGER);")
        conn.commit()

        appender = Appender(conn, "main", "test")
        appender.append_row((1,))
        appender.append_row([2])
        appender.append_row({"i": 3}.values())
        appender.close()

        assert conn.execute("SELECT count(i) FROM test").fetchone() == (3,)
