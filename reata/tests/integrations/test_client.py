import mysql.connector
import pytest

from unittest.mock import Mock

from hypothesis import given, HealthCheck, settings
from mysql.connector import errorcode

from reata.tests.conftest import TEMP_DB
from reata.tests.helpers import dummies
from reata.client import autocommit
from reata.schema import TableSchema

TEST_COLUMNS = {
    "id": "INTEGER(16) NOT NULL AUTO_INCREMENT",
    "name": "VARCHAR(32) NOT NULL",
    "age": "INTEGER(3) DEFAULT NULL",
    "computed": "INTEGER(4) GENERATED ALWAYS AS (`age` * 2) STORED",
}
TEST_TABLE = TableSchema(
    "test_table",
    TEST_COLUMNS,
    primary_key=(list(TEST_COLUMNS.keys())[0], ),
)
TEST_DATA = (
    ("Bob", 42),
    ("Karen", 56),
    ("Earl", 40),
    ("Natalie", 26),
    ("John", 35),
)

# In order to test that the `closing` function from `contextlib` is actually
# closing cursor objects, we can add the following assert to the methods we
# wish to test. This can only be tested from within the methods themselves.
# try:
#     cursor.execute(stmt)
# except mysql.connector.Error as err:
#     assert err.errno == 2055
#


def test_autocommit():
    """Verify that the decorator is calling enter and exit methods"""

    class MockClass:

        @autocommit
        def mock_method(self, x):
            return x + 1

    MockClass.__enter__ = Mock(name="__enter__")
    MockClass.__exit__ = Mock(name="__exit__")

    # Pass a dummy value to the method to ensure that the decorator returns as
    # expected.
    assert MockClass().mock_method(1, autocommit=False) == 2
    MockClass.__enter__.assert_not_called()
    MockClass.__exit__.assert_not_called()

    assert MockClass().mock_method(1) == 2
    MockClass.__enter__.assert_called()
    MockClass.__exit__.assert_called()


class TestMySQLClient:

    def test_create_database(self, client):
        """Verify that the database gets created"""

        # We first need to assert that the database does not already exist.
        # The `create_database` function will not raise an error when
        # attempting to create an existing database, which means our test will
        # pass silently if we attempt to create a database that is already on
        # the server.
        assert client.database_exists(TEMP_DB) is False
        client.create_database(TEMP_DB)

        # Since the `database_exists` method can only return one of two boolean
        # values, we are effectively validating its functionality for free by
        # using it in two opposing contexts. If both assertions are true, we
        # can be relatively certain that the method is functioning as intended.
        assert client.database_exists(TEMP_DB)

    def test_use(self, client):
        """Verify that the function selects the appropriate database"""
        assert client.database_exists(TEMP_DB) is False
        # If the `auto_create` argument is false, then attempting to select a
        # non-existent database should raise an error.
        with pytest.raises(mysql.connector.Error) as err:
            client.use(TEMP_DB)
            assert err.errno == errorcode.ER_BAD_DB_ERROR

        client.create_database(TEMP_DB)
        assert client.database_exists(TEMP_DB)
        client.use(TEMP_DB)
        assert client.database == TEMP_DB

    def test_use_autocreate(self, client):
        """
        Ensure that any non-existent databases which we try to select get
        created when 'auto_create' is true.
        """
        assert client.database_exists(TEMP_DB) is False
        client.use(TEMP_DB, auto_create=True)
        assert client.database_exists(TEMP_DB)
        assert client.database == TEMP_DB

    @given(dummies.sql_tables)
    @settings(
        max_examples=56,
        suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_create_table(self, client, schema):
        """Verify that each table gets created"""
        client.use(TEMP_DB, auto_create=True)

        # Because we're pulling from a strategy at random, the function may
        # attempt to create a table with a given name more than once. This is
        # acceptable, so long as the first attempt is always successful.
        client.create_table(schema)
        assert client.table_exists(schema.name)

    def test_add_index(self, client):
        """Verify that the index gets added to the specified column"""
        client.use(TEMP_DB, auto_create=True)
        client.create_table(TEST_TABLE)
        index_name = "name_idx"
        index_column = "name"
        assert not client.index_exists(
            TEST_TABLE.name,
            index_name,
            index_column
        )
        client.add_index(TEST_TABLE.name, index_name, index_column)
        assert client.index_exists(TEST_TABLE.name, index_name, index_column)

    def test_table_names(self, client):
        """
        Verify that the set which gets returned contains all of the tables
        that have been created. In this case, only one table should be
        contained in the set.
        """
        client.use(TEMP_DB, auto_create=True)
        assert client.table_names == set()
        client.create_table(TEST_TABLE)
        assert client.table_names == {TEST_TABLE.name}

    @pytest.mark.parametrize("include_virtual, expected",
        ([True, 4],
         [False, 3]))
    def test_column_count(self, include_virtual, expected, client):
        """
        The method should return an integer whose value is equal to the number
        of columns in the table we just created.
        """
        client.use(TEMP_DB, auto_create=True)
        client.create_table(TEST_TABLE)
        col_count = client.column_count(
            TEST_TABLE.name,
            include_virtual=include_virtual,
        )
        assert isinstance(col_count, int)
        assert col_count == expected

    @pytest.mark.parametrize("include_virtual, include_auto, expected",
        ([True, True, tuple(TEST_COLUMNS.keys())],
         [True, False, tuple(TEST_COLUMNS.keys())[1:]],
         [False, True, tuple(TEST_COLUMNS.keys())[:-1]],
         [False, False, tuple(TEST_COLUMNS.keys())[1:-1]]))
    def test_column_names(
            self,
            include_virtual,
            include_auto,
            expected,
            client
            ):
        """
        Verify that the method always returns the column names we would
        expect to see, given the combination of parameters supplied.
        """
        client.use(TEMP_DB, auto_create=True)
        client.create_table(TEST_TABLE)
        col_names = client.column_names(
            table_name=TEST_TABLE.name,
            include_virtual=include_virtual,
            include_auto=include_auto
        )
        assert col_names == expected

    def test_fetch_rows(self, client):
        """The return object should be identical to our test data object"""
        client.use(TEMP_DB, auto_create=True)
        client.create_table(TEST_TABLE)
        client.bulk_insert(
            TEST_TABLE.name,
            ["name", "age"],
            TEST_DATA,
            update_method=None,
        )
        rows = tuple(client.fetch_rows(TEST_TABLE.name, ("name", "age")))
        assert rows == TEST_DATA

    @pytest.mark.parametrize("update_method", ["upsert", "replace"])
    def test_bulk_insert(self, client, update_method):
        """
        Verify that the records get inserted. Updated values should not
        match values in our initial data set.
        """
        client.use(TEMP_DB, auto_create=True)
        client.create_table(TEST_TABLE)
        client.bulk_insert(
            TEST_TABLE.name,
            ["name", "age"],
            TEST_DATA,
        )
        result_1 = {
            k: [i, x, y]
            for i, k, x, y in (
                x for x in client.fetch_rows(TEST_TABLE.name)
            )
        }
        new_data = (("Bob", 43), ("Earl", 41),)
        client.bulk_insert(
            TEST_TABLE.name,
            ["name", "age"],
            new_data,
            update_method=update_method,
        )
        result_2 = {
            k: [i, x, y]
            for i, k, x, y in (
                x for x in client.fetch_rows(TEST_TABLE.name)
            )
        }
        assert result_1["Bob"][1] != result_2["Bob"][1]
        assert result_1["Earl"][1] != result_2["Earl"][1]

        assert result_1["Natalie"][1] == result_2["Natalie"][1]
