import pytest
from pytest_mock import MockerFixture


@pytest.fixture
def db_table_mock(mocker: MockerFixture, db_manager_mock):
    db_table_mock = mocker.MagicMock()

    db_table_mock.where.return_value = db_table_mock
    db_table_mock.order_by.return_value = db_table_mock
    db_table_mock.retrieve.return_value = db_table_mock

    db_manager_mock.return_value.table.return_value = db_table_mock
    db_manager_mock.return_value.retrieve_table.return_value = db_table_mock

    return db_table_mock


@pytest.fixture
def db_manager_mock(safe_module_patch):
    return safe_module_patch("DatabaseManager")
