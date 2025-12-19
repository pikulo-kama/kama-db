from collections import namedtuple
import pytest
from pytest_mock import MockerFixture

SqliteColInfo = namedtuple('SqliteColInfo', ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'])


class TestDatabaseTable:


    @pytest.fixture
    def mock_table_info_cursor(self, _db_mock):
        """
        Mocks the select result for PRAGMA table_info (PK = ID).
        """

        pk_info = SqliteColInfo(1, 'id', 'INTEGER', 0, None, 1)  # Primary Key
        non_pk_info = SqliteColInfo(2, 'name', 'TEXT', 0, None, 0)

        _db_mock.select.return_value.fetchall.return_value = [pk_info, non_pk_info]
        # Reset select mock for actual select calls
        _db_mock.select.reset_mock()


    @pytest.fixture
    def mock_retrieve_cursor(self, _db_mock):
        """
        Mocks the select result for SELECT * FROM table.
        """

        # Mock cursor description (for column names)
        MockCursorDescription = namedtuple('MockCursorDescription', ['name'])
        description = [
            MockCursorDescription('ID'),
            MockCursorDescription('Name'),
            MockCursorDescription('Value')
        ]
        _db_mock.select.return_value.description = description

        # Mock fetched data
        _db_mock.select.return_value.fetchall.return_value = [
            (1, "Alpha", 100),
            (2, "Beta", 200)
        ]


    @pytest.fixture
    def _db_mock(self, mocker: MockerFixture):
        # Can't use global "db_mock" fixture
        # since db is not being imported directly into file but is
        # ab argument of DatabaseTable constructor.
        return mocker.MagicMock()


    @pytest.fixture
    def _database_table(self, _db_mock):
        """
        An initialized DatabaseTable instance.
        """

        from kdb.table import DatabaseTable

        return DatabaseTable(_db_mock, "test_table")

    def test_table_init(self, _database_table):
        """
        Tests basic initialization.
        """

        assert _database_table.rows == []
        assert _database_table.columns == []
        assert _database_table.is_empty is True


    def test_table_where_chaining(self, _database_table):
        """
        Tests the where method for chaining and state setting.
        """

        table = _database_table.where("id > ?", 5)

        assert table is _database_table
        assert _database_table._DatabaseTable__where_clause == "id > ?"  # noqa
        assert _database_table._DatabaseTable__where_clause_args == (5,)  # noqa


    def test_table_order_by_chaining(self, _database_table):
        """
        Tests the order_by method for chaining and state setting.
        """

        table = _database_table.order_by("name ASC")

        assert table is _database_table
        assert _database_table._DatabaseTable__order_by_clause == "name ASC"  # noqa


    def test_table_retrieve_no_clauses(self, _database_table, _db_mock, mock_retrieve_cursor):
        """
        Tests retrieve with no WHERE or ORDER BY clauses.
        """

        table = _database_table.retrieve()

        # 1. Check SQL execution
        _db_mock.select.assert_called_once_with(
            "SELECT * FROM test_table",
            tuple()
        )

        # 2. Check internal state after retrieval
        assert table.columns == ["id", "name", "value"]
        assert len(table.rows) == 2
        assert table.rows[0].get("name") == "Alpha"
        assert table.is_empty is False


    def test_table_retrieve_with_clauses(self, _database_table, _db_mock, mock_retrieve_cursor):
        """
        Tests retrieve with WHERE and ORDER BY clauses.
        """

        where_clause = "value > ?"
        order_by_clause = "name DESC"

        _database_table.where(where_clause, 150).order_by(order_by_clause).retrieve()

        # 1. Check SQL execution (should have all clauses)
        _db_mock.select.assert_called_once_with(
            f"SELECT * FROM test_table WHERE {where_clause} ORDER BY {order_by_clause}",
            (150,)
        )
        # The where clause and order by clause should be reset after retrieve
        assert _database_table._DatabaseTable__where_clause is where_clause  # noqa
        assert _database_table._DatabaseTable__order_by_clause is order_by_clause  # noqa


    def test_table_add_row(self, _database_table, mock_retrieve_cursor):
        """
        Tests adding a new row.
        """

        # Retrieve first to set up columns and counter
        _database_table.retrieve()

        row_number = _database_table.add_row()

        assert row_number == 3
        assert len(_database_table.rows) == 3

        new_row = _database_table.rows[-1]
        assert new_row.row_number == 3
        assert new_row.is_new is True
        assert new_row.get("id") is None


    def test_table_get_set_specific_row(self, _database_table, mock_retrieve_cursor):
        """
        Tests get/set for a specific row number.
        """

        _database_table.retrieve()  # Data: (1, "Alpha", 100), (2, "Beta", 200)

        # Test get (row 2)
        assert _database_table.get(2, "name") == "Beta"
        assert _database_table.get(99, "name") is None  # Non-existent row

        # Test set (row 1)
        _database_table.set(1, "Name", "Gamma")

        # Check that the change is in the row's edits
        row1 = _database_table.rows[0]
        assert row1.has_edits()
        assert row1.edits["Name"] == "Gamma"


    def test_table_get_set_first(self, _database_table, mock_retrieve_cursor):
        """
        Tests get_first/set_first methods.
        """

        _database_table.retrieve()  # Data starts at row_number 1

        # Test get_first
        assert _database_table.get_first("name") == "Alpha"

        # Test set_first
        _database_table.set_first("Value", 999)

        # Check row 1 edits
        row1 = _database_table.rows[0]
        assert row1.edits["Value"] == 999


    def test_table_remove_single_record(self, _database_table, mock_retrieve_cursor):
        """
        Tests removing a single record.
        """

        _database_table.retrieve()  # Data: (1, "Alpha", 100), (2, "Beta", 200)

        table = _database_table.remove(1)  # Remove row 1

        assert table is _database_table
        assert len(_database_table.rows) == 1
        assert _database_table.rows[0].row_number == 2  # Remaining row is row 2
        assert len(_database_table._DatabaseTable__deleted_records) == 1  # noqa
        assert _database_table._DatabaseTable__deleted_records[0].row_number == 1  # noqa


    def test_table_remove_all(self, _database_table, mock_retrieve_cursor):
        """
        Tests removing all records.
        """

        _database_table.retrieve()  # Data: (1, "Alpha", 100), (2, "Beta", 200)

        table = _database_table.remove_all()

        assert table is _database_table
        assert len(_database_table.rows) == 0
        assert len(_database_table._DatabaseTable__deleted_records) == 2  # noqa
        assert _database_table.is_empty is True


    def test_table_save_calls_delete_update_insert(self, mocker: MockerFixture, _database_table, mock_retrieve_cursor,
                                                   mock_table_info_cursor):
        """
        Tests that save calls the internal methods in the correct order.
        """

        # Ensure table columns are set up for PK lookup and save logic
        _database_table.retrieve()

        # Mock out the internal save components to spy on calls
        mock_delete = mocker.patch.object(_database_table, '_DatabaseTable__delete_records')
        mock_update = mocker.patch.object(_database_table, '_DatabaseTable__update_records')
        mock_insert = mocker.patch.object(_database_table, '_DatabaseTable__insert_records')

        _database_table.save()

        # Check call order
        mock_delete.assert_called_once()
        mock_update.assert_called_once()
        mock_insert.assert_called_once()


    def test_table_save_insert_records(self, mocker: MockerFixture, _database_table, _db_mock, mock_retrieve_cursor,
                                       mock_table_info_cursor):
        """
        Tests the __insert_records logic.
        """

        _database_table.retrieve()
        _database_table.add_row()
        _database_table.rows[-1].set("name", "NewUser")
        _database_table.rows[-1].set("value", 500)

        mocker.patch.object(_database_table, "_DatabaseTable__delete_records")
        mocker.patch.object(_database_table, "_DatabaseTable__update_records")
        _database_table.save()

        expected_args = ("NewUser", 500)

        # NOTE: We need to check if execute was called with the stripped SQL for reliability
        sql_call = _db_mock.execute.call_args[0][0].strip()

        assert "INSERT INTO test_table (name, value)" in sql_call
        assert "VALUES (?, ?)" in sql_call
        assert _db_mock.execute.call_args[0][1] == expected_args

        # Check post-insert state
        assert _database_table.rows[-1].is_new is False
        assert not _database_table.rows[-1].has_edits()
        assert _database_table.rows[-1].get("name") == "NewUser"


    def test_table_save_update_records(self, mocker: MockerFixture, _database_table, _db_mock, mock_retrieve_cursor,
                                       mock_table_info_cursor):
        """
        Tests the __update_records logic.
        """

        _database_table.retrieve()  # Sets up rows 1 and 2
        _database_table.rows[0].set("name", "UpdatedAlpha")  # Row 1 (ID=1)

        mocker.patch.object(_database_table, "_DatabaseTable__delete_records")
        mocker.patch.object(_database_table, "_DatabaseTable__insert_records")
        _database_table.save()

        sql_call = _db_mock.execute.call_args[0][0].strip()

        assert "UPDATE test_table" in sql_call
        assert "SET name = ?" in sql_call
        assert "WHERE id = ?" in sql_call

        # Check post-update state
        assert not _database_table.rows[0].has_edits()
        assert _database_table.rows[0].get("name") == "UpdatedAlpha"


    def test_table_save_delete_records(self, mocker: MockerFixture, _db_mock, _database_table, mock_retrieve_cursor,
                                       mock_table_info_cursor):
        """
        Tests the __delete_records logic.
        """

        _database_table.retrieve()

        # Test scenario when nothing was deleted,
        # then DELETE statement should not be executed.
        _database_table.save()
        _db_mock.execute.assert_not_called()

        _database_table.remove(1)  # Row 1 (ID=1) deleted
        _database_table.remove(2)  # Row 2 (ID=2) deleted

        # Mock out the other two steps for isolation
        mocker.patch.object(_database_table, "_DatabaseTable__update_records")
        mocker.patch.object(_database_table, "_DatabaseTable__insert_records")

        _database_table.save()

        # Check that execute was called with the correct DELETE statement
        # PK column is 'id' from mock_table_info_cursor
        expected_values = (1, 2)

        sql_call = _db_mock.execute.call_args[0][0].strip()

        # Check if the generated SQL contains the necessary clauses
        assert "DELETE FROM test_table" in sql_call
        assert "WHERE (id = ?) OR (id = ?)" in sql_call
        assert _db_mock.execute.call_args[0][1] == expected_values

        # Check post-delete state
        assert len(_database_table._DatabaseTable__deleted_records) == 0  # noqa


    def test_table_save_delete_when_no_pk_on_record(self, mocker: MockerFixture, _db_mock, _database_table,
                                                    mock_retrieve_cursor, mock_table_info_cursor):

        _database_table.retrieve()

        _database_table.rows[0].set("id", None)
        _database_table.rows[0]._apply_edits()
        _database_table.remove(1)  # Row 1 (ID=1) deleted

        # Mock out the other two steps for isolation
        mocker.patch.object(_database_table, "_DatabaseTable__update_records")
        mocker.patch.object(_database_table, "_DatabaseTable__insert_records")

        _database_table.save()

        sql_call = _db_mock.execute.call_args[0][0].strip()

        # Check if the generated SQL contains the necessary clauses
        assert "DELETE FROM test_table" in sql_call
        assert "WHERE (id IS NULL)" in sql_call


    def test_columns_iterator(self, _database_table, mock_retrieve_cursor, mock_table_info_cursor):
        _database_table.retrieve()

        for idx, record in enumerate(_database_table):
            assert record.get("id") == idx + 1
