import sqlite3

from kdb.table import DatabaseTable
from kutil.logger import get_logger


_logger = get_logger(__name__)


class DatabaseManager:
    """
    Wrapper for sqlite connection.
    Used for low level database interactions.
    """

    def __init__(self, database_path: str):
        self.__database_path = database_path

    def retrieve_table(self, table_name: str) -> DatabaseTable:
        """
        Used to create table object to perform
        CRUD operations on table data.

        Will automatically retrieve all table data.
        """
        return self.table(table_name).retrieve()

    def table(self, table_name: str) -> DatabaseTable:
        """
        Used to create table object to perform
        CRUD operations on table data.
        """
        return DatabaseTable(self, table_name)

    def execute(self, sql: str, *args, **kwargs):
        """
        Used to execute edit statements.
        """

        _logger.debug("Executing alter statement.")
        _logger.debug("SQL: %s", sql)
        _logger.debug("args=%s, kw=%s", args, kwargs)

        connection = self.connection()
        connection.execute(sql, *args, **kwargs)  # noqa
        connection.commit()

    def select(self, sql: str, *args, **kwargs):
        """
        Used to execute select statements.
        """

        _logger.debug("Executing select statement.")
        _logger.debug("SQL: %s", sql)
        _logger.debug("args=%s, kw=%s", *args, kwargs)

        connection = self.connection()
        cursor = connection.cursor()
        cursor.execute(sql, *args, **kwargs)  # noqa

        return cursor

    def connection(self):
        """
        Used to create sqlite connection.
        """
        return sqlite3.connect(self.__database_path)
