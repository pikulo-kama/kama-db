from kutil.logger import get_logger

_logger = get_logger(__name__)


class DatabaseRow:
    """
    Table row object.
    Used to perform operations on row data.
    """

    def __init__(self, table_name: str, row_number: int, data: tuple, columns: list[str]):

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
        """
        return self.__data.get(column_name.lower())

    def set(self, column_name: str, column_value):
        """
        Used to set column value for row.
        """
        self.__edits[column_name] = column_value

    @property
    def is_new(self):
        """
        Used to check if row has been inserted
        or if it was retrieved from database.
        """
        return self.__is_new

    @is_new.setter
    def is_new(self, is_new: bool):
        """
        Used to mark row as new and
        vice versa.
        """
        self.__is_new = is_new

    @property
    def edits(self):
        """
        Used to get all row edits.
        Map of column name -> column value.
        """
        return self.__edits

    @property
    def row_number(self):
        """
        Used to get number of row.
        """
        return self.__row_number

    def has_edits(self):
        """
        Used to check whether row has
        any edits.
        """
        return len(self.__edits) > 0

    def _apply_edits(self):
        """
        Used to apply edits to table row.
        """

        _logger.debug("Applying edits to row data.")
        _logger.debug("Before: %s", self.__data)

        self.__data = {**self.__data, **self.__edits}
        self.__edits.clear()

        _logger.debug("After: %s", self.__data)

    def to_json(self):
        """
        Used to get row data in
        JSON format.
        """
        return self.__data

    def __str__(self):
        return f"{self.__table_name}: {self.to_json()}"
