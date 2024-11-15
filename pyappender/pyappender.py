from dataclasses import dataclass
import datetime
import json
import logging
import os
import time

import sqlite3
import duckdb
import uuid

from pyappender import constants
from pyappender.errors import (
    AppendAfterCloseError,
    AppenderDoubleCloseError,
    ColumnCountError,
    TableNotFoundError,
)

logger = logging.getLogger(__name__)


@dataclass
class TableSchema:
    table_name: str
    column_names: list[str]
    column_types: list[str]


class SQLiteBuffer:
    db_filepath: str
    table: str
    table_schema: TableSchema
    row_count: int = 0

    _conn: sqlite3.Connection
    _cur: sqlite3.Cursor

    def __init__(self, table: str, table_schema: TableSchema):
        self.db_filepath = f"tmp_pyappender_buffer_{uuid.uuid4()}.db"
        self.table = table
        self.table_schema = table_schema

        self._conn = sqlite3.connect(self.db_filepath)
        self._cur = self._conn.cursor()

        self._create_table(table_schema)
        self._resister_adapters()

    def _create_table(self, schema: TableSchema) -> None:
        self._cur.execute(f"DROP TABLE IF EXISTS {schema.table_name}")

        self._cur.execute(
            f"""
            CREATE TABLE {schema.table_name} (
                {', '.join([f"{col_name} {col_type}" for col_name, col_type in zip(schema.column_names, schema.column_types)])}
            )
            """
        )
        self._conn.commit()

        self.table = schema.table_name

    def _resister_adapters(self) -> None:
        """Register the adapter for non-native SQLite types.
        Ref: https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values
        """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.hex)
        sqlite3.register_adapter(datetime.date, lambda d: d.isoformat())
        sqlite3.register_adapter(datetime.datetime, lambda t: t.isoformat())

        def adapt_list(lst):
            if type(lst[0]) == str:
                return f"[{', '.join(lst)}]"
            return f"[{', '.join([str(i) for i in lst])}]"

        sqlite3.register_adapter(list, adapt_list)

    def append_row(self, row: list) -> None:
        if len(row) != len(self.table_schema.column_names):
            raise ColumnCountError(len(self.table_schema.column_names), len(row))

        self._cur.execute(
            f"""INSERT INTO {self.table} (
                {', '.join([col for col in self.table_schema.column_names])}
            ) VALUES (
                {', '.join(['?' for _ in row])}
            )""",
            tuple(row),
        )

        self.row_count += 1

    def commit(self) -> None:
        """Commit all pending insertions.
        We do not want to commit each insertion one by one for the best performance.
        """
        self._conn.commit()

    def close(self) -> None:
        """Commit all the insertions and drop the sqlite database."""
        self._conn.commit()
        self._conn.close()

    def __del__(self):
        # Delete the sqlite database
        os.remove(self.db_filepath)
        pass


class Appender:
    conn: duckdb.DuckDBPyConnection
    schema: str
    table: str
    closed: bool = False

    buffer: SQLiteBuffer

    def __init__(self, conn: duckdb.DuckDBPyConnection, schema: str, table: str):
        self.conn = conn
        self.schema = schema
        self.table = table

        self._config_duckdb_session()

        # Mirror the duckdb table into sqlite
        table_schema = self._get_duckdb_table_schema()
        table_schema = _convert_duckdb_to_sqlite_schema(table_schema)
        self.buffer = SQLiteBuffer(table, table_schema)

    def append_row(self, row: list) -> None:
        if self.closed:
            raise AppendAfterCloseError()

        self.buffer.append_row(row)

        # If the buffer reaches autocommit limit, flush it to duckdb and create a new
        # buffer
        if self.buffer.row_count >= constants.DEFAUT_AUTOCOMMIT_ROW_COUNT:
            self.flush()
            new_buffer = SQLiteBuffer(self.buffer.table, self.buffer.table_schema)
            self.buffer.close()
            self.buffer = new_buffer

    def flush(self) -> None:
        """Flush the pending data from the buffer to the duckdb table and commit the
        change.

        Prefer to call close() when you are done with the appender instead of manually
        calling this method.
        """
        self.buffer.commit()
        self.write_data_to_duckdb()

    def close(self) -> None:
        """Flush the changes made by the appender and close it."""
        if self.closed:
            raise AppenderDoubleCloseError()

        self.flush()
        self.buffer.close()
        self.closed = True

    def write_data_to_duckdb(self) -> None:
        """Write data from buffer to duckdb table."""

        start = time.time()
        self.conn.execute(
            f"INSERT INTO {self.table} SELECT * FROM sqlite_scan(?, ?);",
            (
                self.buffer.db_filepath,
                self.table,
            ),
        )
        self.conn.commit()

        logger.debug(f"Time to write data to duckdb: {time.time() - start}s")

    def _config_duckdb_session(self):
        """Set timezone to UTC to avoid timezone conversion issue between DuckDB and
        SQLite. This problem also exist with other systems such as Apache Arrow.
        Reference: https://github.com/duckdb/duckdb/issues/9381

        Note: This only set the timezone per session, so it won't affect global setting.
        """
        self.conn.execute("SET timezone='UTC';")

    def _get_duckdb_table_schema(self) -> TableSchema:
        rows = self.conn.execute(
            f"""SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = ?
                        AND table_name = ?
                """,
            (
                self.schema,
                self.table,
            ),
        ).fetchall()

        if len(rows) == 0:
            raise TableNotFoundError()

        return TableSchema(
            self.table,
            column_names=[row[0] for row in rows],
            column_types=[row[1] for row in rows],
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _convert_duckdb_to_sqlite_schema(schema: TableSchema):
    new_column_types = []
    for t in schema.column_types:
        if t.endswith("[]"):
            # SQLite doesn't support list/array types, so default to VARCHAR
            sqlite_type = "VARCHAR"
        else:
            sqlite_type = t
        new_column_types.append(sqlite_type)

    return TableSchema(schema.table_name, schema.column_names, new_column_types)
