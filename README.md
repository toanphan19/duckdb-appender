# DuckDB PyAppender

A DuckDB appender for Python.

## Usage

```py
conn = duckdb.connect()

appender = Appender(conn, schema="main", table="users")
appender.append_row([1, "John"])
appender.append_row([2, "Doe"])
appender.close()

# Can also use it with context manager:
with Appender(conn, schema="main", table="users") as appender:
  appender.append_row([3, "foo"])
```

See more in the `/examles` folder.

## Why use this

- Much faster than simple INSERT statements.
- Versus storing data in a pandas DataFrame and load into DuckDB: while pandas loads the entire dataset into memory, this appender uses SQLite as a buffer that regularly flush itself (every 204,800 rows, similar to the official C++ appender)
  - This prevents memory overload on large data aggregation.
  - A little bit of crash protection - since it commits data every 204,800 rows, you'll preserve most of your progress if something goes wrong.

## Local development

This repo uses [uv](https://github.com/astral-sh/uv) to manage dependencies and
virtual environment. Dependencies are written in `pyproject.toml`, so other
tools supporting it can be used as well.

### Run test

This repo use [pytest](https://docs.pytest.org/en/stable/). To run tests within an uv environment, run:

```bash
uv run pytest
```
