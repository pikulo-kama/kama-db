from typing import TYPE_CHECKING, Callable, Iterator, Any
from kutil.logger import get_logger

from kdb.row import DatabaseRow

if TYPE_CHECKING:
    from kdb.manager import DatabaseManager


_logger = get_logger(__name__)


class DatabaseTable:
    """Database table wrapper used to perform operations on table data.

    This class provides an abstraction over a specific database table, allowing for
    filtering, sorting, and local tracking of record changes (inserts, updates,
    and deletes) before persisting them to the database.
    """

    def __init__(self, db: "DatabaseManager", table_name: str):
        """Initializes the DatabaseTable with a database manager and table name.

        Args:
            db: The DatabaseManager instance used for SQL execution.
            table_name: The name of the table in the SQLite database.
        """

        self.__db = db
        self.__record_counter = 0
        self.__where_clause = None
        self.__where_clause_args = []
        self.__order_by_clause = None
        self.__table_name = table_name
        self.__records: list[DatabaseRow] = []
        self.__deleted_records: list[DatabaseRow] = []
        self._columns: list[str] = []

    def __iter__(self) -> Iterator[DatabaseRow]:
        """
        Iterates over the local list of database records.
        """
        return iter(self.__records)

    def where(self, where_clause: str, *args: Any):
        """Applies a WHERE clause filter for the next retrieval.

        Args:
            where_clause: The SQL WHERE condition (e.g., 'id = ?').
            *args: Positional arguments to bind to the WHERE clause.

        Returns:
            DatabaseTable: The current instance for method chaining.
        """

        self.__where_clause = where_clause
        self.__where_clause_args = args

        return self

    def order_by(self, order_by_clause: str):
        """Sets the ORDER BY clause for the next retrieval.

        Args:
            order_by_clause: The SQL column and direction (e.g., 'name ASC').

        Returns:
            DatabaseTable: The current instance for method chaining.
        """

        self.__order_by_clause = order_by_clause
        return self

    def retrieve(self):
        """Retrieves table data from the database based on active filters.

        Clears existing local records and populates them with data fetched
        via the DatabaseManager.

        Returns:
            DatabaseTable: The current instance with populated records.
        """

        sql = f"SELECT * FROM {self.__table_name}"
        retrieve_args = tuple()

        if self.__where_clause is not None:
            sql += f" WHERE {self.__where_clause}"
            retrieve_args = tuple(self.__where_clause_args)

        if self.__order_by_clause is not None:
            sql += f" ORDER BY {self.__order_by_clause}"

        cursor = self.__db.select(sql, retrieve_args)
        self._columns = [str(description[0]).lower() for description in cursor.description]
        self.__records.clear()
        self.__record_counter = 0

        for row_data in cursor.fetchall():
            self.__record_counter += 1
            row = DatabaseRow(self.__table_name, self.__record_counter, row_data, self._columns)
            self.__records.append(row)

        _logger.debug("Data for table %s have been retrieved.", self.__table_name)
        _logger.debug("Table columns: %s", self._columns)
        _logger.debug("Record count: %d", self.__record_counter)

        return self

    @property
    def is_empty(self) -> bool:
        """
        Checks if the table's local record list is empty.
        """
        return len(self.rows) == 0

    @property
    def rows(self) -> list[DatabaseRow]:
        """
        Returns the list of DatabaseRow objects currently in memory.
        """
        return self.__records

    @property
    def columns(self) -> list[str]:
        """
        Returns the list of table column names.
        """
        return self._columns

    def add(self, **values) -> DatabaseTable:
        row = self.add_row()

        for column_name, value in values.items():
            self.set(row, column_name, value)

        return self

    def add_row(self) -> int:
        """Adds a new, empty row to the local record list.

        The row is marked as 'new' and will not be persisted in the database
        until save() is called.

        Returns:
            int: The row number assigned to the new row.
        """

        self.__record_counter += 1
        row = DatabaseRow(self.__table_name, self.__record_counter, tuple(), self._columns)
        row.is_new = True

        self.__records.append(row)
        _logger.debug("New row has been added to %s. Row number = %d", self.__table_name, row.row_number)
        return row.row_number

    def get_first(self, column_name: str) -> Any:
        """Retrieves a column value from the first row in the dataset.

        Args:
            column_name: The name of the column to retrieve.

        Returns:
            The column value or None if the dataset is empty.
        """
        return self.get(1, column_name)

    def set_first(self, column_name: str, column_value: Any) -> None:
        """Sets a column value for the first row in the dataset.

        Args:
            column_name: The name of the column to modify.
            column_value: The new value to assign.
        """
        self.set(1, column_name, column_value)

    def get(self, row_number: int, column_name: str) -> Any:
        """Gets a column value from a specific row identified by row_number.

        Args:
            row_number: The numerical identifier of the row.
            column_name: The name of the column to retrieve.

        Returns:
            The column value, or None if no matching row is found.
        """

        for record in self.__records:
            if record.row_number == row_number:
                return record.get(column_name)

        return None

    def set(self, row_number: int, column_name: str, column_value: Any) -> None:
        """Sets a column value for a specific row identified by row_number.

        Args:
            row_number: The numerical identifier of the row.
            column_name: The name of the column to modify.
            column_value: The new value to assign.
        """

        for record in self.__records:
            if record.row_number == row_number:
                record.set(column_name, column_value)

    def save(self) -> None:
        """Persists all local changes to the database.

        This includes deleting records marked for removal, updating modified
        records, and inserting new records.
        """

        _logger.debug("Saving table data for %s.", self.__table_name)
        self.__delete_records()
        self.__update_records()
        self.__insert_records()

    def remove(self, row_number: int):
        """Marks a specific row for removal from the database.

        Args:
            row_number: The numerical identifier of the row to remove.

        Returns:
            DatabaseTable: The current instance for method chaining.
        """

        self.__remove_internal(lambda record: record.row_number == row_number)
        return self

    def remove_all(self):
        """Marks all local records for removal from the database.

        Returns:
            DatabaseTable: The current instance for method chaining.
        """

        self.__remove_internal(lambda record: True)
        return self

    def __delete_records(self):
        """
        Handles the deletion of records during the save process.
        """

        pk_columns = self.__get_pk_columns()
        pk_filter_sql = []
        pk_filter_values = []

        if len(self.__deleted_records) == 0:
            return

        for record in self.__deleted_records:
            pk_filter_single = []

            for pk_column in pk_columns:
                pk_value = record.get(pk_column)

                if pk_value is None:
                    pk_filter_single.append(f"{pk_column} IS NULL")

                else:
                    pk_filter_single.append(f"{pk_column} = ?")
                    pk_filter_values.append(pk_value)

            pk_filter_sql.append(f"({" AND ".join(pk_filter_single)})")

        self.__db.execute(f"""
            DELETE FROM {self.__table_name}
            WHERE {" OR ".join(pk_filter_sql)}
        """, tuple(pk_filter_values))

        self.__deleted_records.clear()

    def __update_records(self):
        """
        Handles the update of modified records during the save process.
        """

        pk_columns = self.__get_pk_columns()

        for record in self.__records:
            if not record.has_edits() or record.is_new:
                continue

            update_fields = ", ".join([f"{column_name} = ?" for column_name in record.edits.keys()])
            values = tuple(record.edits.values())

            sql = f"""
                UPDATE {self.__table_name} 
                SET {update_fields}
            """

            if len(pk_columns) > 0:
                filter_conditions = []
                filter_values = []

                for pk_column in pk_columns:
                    filter_conditions.append(f"{pk_column} = ?")
                    filter_values.append(record.get(pk_column))

                sql += f" WHERE {" AND ".join(filter_conditions)}"
                values += tuple(filter_values)

            self.__db.execute(sql, values)
            record._apply_edits()  # noqa

    def __insert_records(self):
        """
        Handles the insertion of new records during the save process.
        """

        for record in self.__records:
            if not record.is_new:
                continue

            insert_field_names = ", ".join([column_name for column_name in record.edits.keys()])
            insert_field_placeholders = ", ".join(["?" for _ in record.edits.keys()])
            insert_field_values = tuple([column_value for column_value in record.edits.values()])

            sql = f"""
                INSERT INTO {self.__table_name} ({insert_field_names})
                VALUES ({insert_field_placeholders})
            """

            self.__db.execute(sql, insert_field_values)
            record._apply_edits()  # noqa
            record.is_new = False

    def __remove_internal(self, remove_condition: Callable[[DatabaseRow], bool]):
        """
        Internal logic for identifying and staging rows for removal.
        """

        for record in self.__records:
            if remove_condition(record):
                self.__deleted_records.append(record)

        for record in self.__deleted_records:
            _logger.debug("Removing row with number %s", record.row_number)

            if record in self.__records:
                self.__records.remove(record)

    def __get_pk_columns(self) -> list[str]:
        """Queries the database to identify primary key columns for the table.

        Returns:
            list[str]: A list of primary key column names.
        """

        cursor = self.__db.select(f"PRAGMA table_info({self.__table_name})")
        columns = cursor.fetchall()
        pk_columns = []

        for col in columns:
            cid, name, type_, notnull, default_value, pk = col
            if pk:
                pk_columns.append(name)

        return pk_columns
