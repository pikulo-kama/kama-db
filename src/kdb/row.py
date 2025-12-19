from kutil.logger import get_logger

_logger = get_logger(__name__)


class DatabaseRow:
    """
    Table row object used to perform operations on row data.

    This class tracks the state of a specific database row, including its
    original data, pending edits, and whether it represents a new record.
    """

    def __init__(self, table_name: str, row_number: int, data: tuple, columns: list[str]):
        """
        Initializes the DatabaseRow instance.

        Args:
            table_name: The name of the table this row belongs to.
            row_number: The numerical index of the row.
            data: The raw data tuple from the database.
            columns: The list of column names for the table.
        """

        self.__table_name = table_name
        self.__row_number = row_number
        self.__data = {}
        self.__edits = {}
        self.__is_new = False

        for index in range(len(columns)):
            column_value = None

            if len(data) > index:
                column_value = data[index]

            self.__data[columns[index].lower()] = column_value

    def get(self, column_name: str):
        """
        Used to get column value from row.

        Args:
            column_name: The name of the column to retrieve.

        Returns:
            The value associated with the column name, or None if not found.
        """
        return self.__data.get(column_name.lower())

    def set(self, column_name: str, column_value):
        """
        Used to set column value for row.

        Args:
            column_name: The name of the column to edit.
            column_value: The new value to assign to the column.
        """
        self.__edits[column_name] = column_value

    @property
    def is_new(self):
        """
        Used to check if row has been inserted or if it was retrieved from database.

        Returns:
            bool: True if the row is marked as new, False otherwise.
        """
        return self.__is_new

    @is_new.setter
    def is_new(self, is_new: bool):
        """
        Used to mark row as new and vice versa.

        Args:
            is_new: The boolean status to set for the row's newness.
        """
        self.__is_new = is_new

    @property
    def edits(self):
        """
        Used to get all row edits.

        Returns:
            dict: A map of column name -> column value for pending edits.
        """
        return self.__edits

    @property
    def row_number(self):
        """
        Used to get number of row.

        Returns:
            int: The index of the row.
        """
        return self.__row_number

    def has_edits(self):
        """
        Used to check whether row has any edits.

        Returns:
            bool: True if there are pending edits, False otherwise.
        """
        return len(self.__edits) > 0

    def _apply_edits(self):
        """
        Used to apply edits to table row data and clear the pending edits map.
        """

        _logger.debug("Applying edits to row data.")
        _logger.debug("Before: %s", self.__data)

        self.__data = {**self.__data, **self.__edits}
        self.__edits.clear()

        _logger.debug("After: %s", self.__data)

    def to_json(self):
        """
        Used to get row data in JSON format.

        Returns:
            dict: The internal dictionary representing the row data.
        """
        return self.__data

    def __str__(self):
        """
        Returns a string representation of the row.
        """
        return f"{self.__table_name}: {self.to_json()}"
