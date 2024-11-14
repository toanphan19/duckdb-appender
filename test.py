import datetime
import duckdb

from pyappender.pyappender import Appender


# conn = duckdb.connect("test3.duckdb")
conn = duckdb.connect()

# Create a table to append to
conn.execute(
    """
    CREATE OR REPLACE TABLE test (
        t1 TIMESTAMPTZ
    );"""
)
conn.commit()

t = datetime.datetime.now(tz=datetime.timezone.utc)
print(t)
conn.execute("INSERT INTO test VALUES (?)", (t,))
conn.commit()

appender = Appender(conn, "main", "test")
appender.append_row([t])
appender.close()

rows = conn.execute("SELECT * FROM test;").fetchall()
print(*rows, sep="\n")
conn.commit()

conn.close()
