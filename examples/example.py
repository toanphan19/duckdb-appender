import duckdb

from duckdb_appender import Appender

if __name__ == "__main__":
    # Set up the source DuckDB table
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR);")
    conn.commit()

    # Use the appender
    appender = Appender(conn, schema="main", table="users")
    appender.append_row([1, "John"])

    data_from_some_api = [
        {"id": 11, "name": "User_11"},
        {"id": 12, "name": "User_12"},
        {"id": 13, "name": "User_13"},
    ]
    for d in data_from_some_api:
        appender.append_row(d.values())

    # Close the appender when you are done inserting.
    appender.close()

    with Appender(conn, schema="main", table="users") as appender:
        appender.append_row([2, "Doe"])

    print(conn.execute("SELECT * FROM users").fetchall())

    conn.close()
