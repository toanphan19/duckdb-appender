class TableNotFoundError(Exception):
    """Cannot find the table in DuckDB"""


class ColumnCountError(Exception):
    def __init__(self, expected: int, actual: int, *args) -> None:
        message = f"invalid number of columns: expected {expected}, got {actual}"
        super().__init__(message, *args)


class AppendAfterCloseError(Exception):
    """Cannot append after having closed the appender"""


class AppenderDoubleCloseError(Exception):
    """The appender is already closed"""
