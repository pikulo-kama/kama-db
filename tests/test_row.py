import pytest


class TestDatabaseRow:

    @pytest.fixture
    def _db_row_data(self):
        """
        Sample data for DatabaseRow initialization.
        """
        return 1, "TestName", 42.5


    @pytest.fixture
    def _db_row_columns(self):
        """
        Sample columns for DatabaseRow initialization.
        """
        return ["ID", "Name", "Value"]


    @pytest.fixture
    def _database_row(self, _db_row_data, _db_row_columns):
        """
        A fully initialized DatabaseRow instance.
        """

        from kdb.table import DatabaseRow

        return DatabaseRow(
            table_name="test_table",
            row_number=5,
            data=_db_row_data,
            columns=_db_row_columns
        )

    def test_str_method(self, _database_row):
        expected = "test_table: {'id': 1, 'name': 'TestName', 'value': 42.5}"
        assert _database_row.__str__() == expected

    def test_database_row_init_and_get(self, _database_row):
        """
        Tests initialization and the case-insensitive get method.
        """

        assert _database_row.row_number == 5
        assert _database_row.get("id") == 1
        assert _database_row.get("NAME") == "TestName"
        assert _database_row.get("value") == 42.5
        assert _database_row.get("nonexistent") is None

    def test_database_row_init_handles_missing_data(self, _db_row_columns):
        """
        Tests init when data tuple is shorter than columns list.
        """

        from kdb.table import DatabaseRow

        row = DatabaseRow("table", 1, (999,), _db_row_columns)

        # Only 'ID' should be set
        assert row.get("id") == 999
        assert row.get("name") is None
        assert row.get("value") is None

    def test_database_row_set_and_edits(self, _database_row):
        """
        Tests setting a value and accessing the 'edits' property.
        """

        _database_row.set("Name", "New Name")
        _database_row.set("Value", 100)

        assert _database_row.edits == {"Name": "New Name", "Value": 100}

    def test_database_row_has_edits(self, _database_row):
        """
        Tests has_edits method.
        """

        assert not _database_row.has_edits()

        _database_row.set("ID", 10)
        assert _database_row.has_edits()

    def test_database_row_is_new_property(self, _database_row):
        """
        Tests is_new getter and setter.
        """

        assert _database_row.is_new is False

        _database_row.is_new = True
        assert _database_row.is_new is True

    def test_database_row_apply_edits(self, _database_row):
        """
        Tests _apply_edits method updates data and clears edits.
        """

        _database_row.set("name", "UpdatedName")
        _database_row.set("new_col", "NewValue")  # Editing an existing key, adding a new key

        assert _database_row.get("name") == "TestName"  # Original data

        _database_row._apply_edits()

        assert _database_row.get("name") == "UpdatedName"  # Edits applied
        assert _database_row.get("new_col") == "NewValue"
        assert not _database_row.has_edits()  # Edits cleared

    def test_database_row_to_json(self, _database_row):
        """
        Tests to_json returns the underlying data dictionary.
        """

        _database_row.set("Name", "Temporary Edit")
        # Edits are NOT reflected until _apply_edits is called

        data = _database_row.to_json()

        assert data == {"id": 1, "name": "TestName", "value": 42.5}
        assert isinstance(data, dict)

    def test_database_row_to_json_after_apply_edits(self, _database_row):
        """
        Tests to_json reflects changes after _apply_edits.
        """

        _database_row.set("name", "Final Name")
        _database_row._apply_edits()

        data = _database_row.to_json()
        assert data["name"] == "Final Name"
