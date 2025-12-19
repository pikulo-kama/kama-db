import sqlite3
from typing import Any

from kdb.table import DatabaseTable
from kutil.logger import get_logger


_logger = get_logger(__name__)


class DatabaseManager:
    """A wrapper for SQLite connections providing high-level database interaction.

    This class manages connection lifecycles and provides convenience methods for
    executing SQL statements and interacting with table-level abstractions.

    Args:
        database_path (str): The file system path to the SQLite database file.
    """

    def __init__(self, database_path: str):
        """Initializes the DatabaseManager with a specific database file.

        Args:
            database_path: The path to the SQLite database.
        """
        self.__database_path = database_path

    def retrieve_table(self, table_name: str) -> DatabaseTable:
        """Creates a table object and immediately fetches its content.

        This is a convenience wrapper around `table()` and `retrieve()`.

        Args:
            table_name: The name of the table to target.

        Returns:
            A DatabaseTable instance populated with data.
        """
        return self.table(table_name).retrieve()

    def table(self, table_name: str) -> DatabaseTable:
        """Initializes a DatabaseTable object for CRUD operations.

        Args:
            table_name: The name of the database table.

        Returns:
            An uninitialized DatabaseTable instance linked to this manager.
        """
        return DatabaseTable(self, table_name)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        """Executes a SQL statement that modifies the database (INSERT, UPDATE, DELETE).

        Automatically manages the connection lifecycle and commits changes.

        Args:
            sql: The SQL statement to execute.
            *args: Positional arguments for SQL parameter binding.
            **kwargs: Keyword arguments for SQL parameter binding.
        """

        _logger.debug("Executing alter statement.")
        _logger.debug("SQL: %s", sql)
        _logger.debug("args=%s, kw=%s", args, kwargs)

        connection = self.connection()
        connection.execute(sql, *args, **kwargs)  # noqa
        connection.commit()

    def select(self, sql: str, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        """Executes a SQL SELECT statement and returns a cursor.

        Args:
            sql: The SELECT query to execute.
            *args: Positional arguments for SQL parameter binding.
            **kwargs: Keyword arguments for SQL parameter binding.

        Returns:
            A sqlite3.Cursor containing the result set.
        """

        _logger.debug("Executing select statement.")
        _logger.debug("SQL: %s", sql)
        _logger.debug("args=%s, kw=%s", args, kwargs)

        connection = self.connection()
        cursor = connection.cursor()
        cursor.execute(sql, *args, **kwargs)  # noqa

        return cursor

    def connection(self) -> sqlite3.Connection:
        """Creates and returns a new SQLite connection.

        Returns:
            A sqlite3.Connection object to the configured database path.
        """
        return sqlite3.connect(self.__database_path)
