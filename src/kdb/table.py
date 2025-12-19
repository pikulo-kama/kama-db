from typing import TYPE_CHECKING, Callable, Iterator
from kutil.logger import get_logger

from kdb.row import DatabaseRow

if TYPE_CHECKING:
    from kdb.manager import DatabaseManager


_logger = get_logger(__name__)


class DatabaseTable:
    """
    Database table wrapper.
    Used to perform operations on table
    data.
    """

    def __init__(self, db: "DatabaseManager",  table_name: str):
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
        return iter(self.__records)

    def where(self, where_clause: str, *args):
        """
        Used to apply WHERE clause filter
        that would be used to retrieve table data.
        """

        self.__where_clause = where_clause
        self.__where_clause_args = args

        return self

    def order_by(self, order_by_clause: str):
        """
        Used to set ORDER BY clause.
        Allows to sort table data when retrieving
        from database.
        """

        self.__order_by_clause = order_by_clause

        return self

    def retrieve(self):
        """
        Used to retrieve table data
        from database.
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
        return len(self.rows) == 0

    @property
    def rows(self) -> list[DatabaseRow]:
        """
        Used to get list of table rows.
        """
        return self.__records

    @property
    def columns(self) -> list[str]:
        """
        Used to get list of table column names.
        """
        return self._columns

    def add_row(self):
        """
        Used to add row to the table.
        Will not immediately persist row in database
        but just add it to the object.
        """

        self.__record_counter += 1
        row = DatabaseRow(self.__table_name, self.__record_counter, tuple(), self._columns)
        row.is_new = True

        self.__records.append(row)
        _logger.debug("New row has been added to %s. Row number = %d", self.__table_name, row.row_number)
        return row.row_number

    def get_first(self, column_name: str):
        """
        Used to get column value of first row
        in dataset.
        """
        return self.get(1, column_name)

    def set_first(self, column_name: str, column_value):
        """
        Used to set column value of first row
        in dataset.
        """
        self.set(1, column_name, column_value)

    def get(self, row_number: int, column_name: str):
        """
        Used to get column value of specific
        table row.
        """

        for record in self.__records:
            if record.row_number == row_number:
                return record.get(column_name)

        return None

    def set(self, row_number: int, column_name: str, column_value):
        """
        Used to set column value of specific
        table row.
        """

        for record in self.__records:
            if record.row_number == row_number:
                record.set(column_name, column_value)

    def save(self):
        """
        Used to persist changes that were made
        to table data in database.
        """

        _logger.debug("Saving table data for %s.", self.__table_name)
        self.__delete_records()
        self.__update_records()
        self.__insert_records()

    def remove(self, row_number: int):
        """
        Used to remove record that corresponds
        provided row number.
        """
        self.__remove_internal(lambda record: record.row_number == row_number)
        return self

    def remove_all(self):
        """
        Used to remove all records from the table.
        """
        self.__remove_internal(lambda record: True)
        return self

    def __delete_records(self):
        """
        Part of save process.
        Used to delete records in database.
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
        Part of save process.
        Used to update records in database.
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
        Part of save process.
        Used to insert new records into database.
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
        Internal row remove method.
        Remove all records that match provided condition.
        """

        for record in self.__records:
            if remove_condition(record):
                self.__deleted_records.append(record)

        for record in self.__deleted_records:
            _logger.debug("Removing row with number %s", record.row_number)

            if record in self.__records:
                self.__records.remove(record)

    def __get_pk_columns(self):
        """
        Used to get name of primary key column
        of table.
        """

        cursor = self.__db.select(f"PRAGMA table_info({self.__table_name})")
        columns = cursor.fetchall()
        pk_columns = []

        for col in columns:
            cid, name, type_, notnull, default_value, pk = col
            if pk:
                pk_columns.append(name)

        return pk_columns
