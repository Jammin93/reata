import mysql.connector
import pytest
import yaml

from contextlib import closing
from pathlib import Path
from typing import Callable

from reata.client import MySQLClient

TEMP_DB = "DELETE_ME"


def read_yaml(path: Path) -> dict:
    """Load the YAML file and return its contents as a dictionary."""
    return yaml.safe_load(path.read_text())


def pytest_addoption(parser):
    """Add commandline options to Pytest"""
    parser.addoption(
        "--go-live",
        action="store_true",
        default=False,
        help="Enable tests which speak to external APIs"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--go-live"):
        return

    skip = pytest.mark.skip(reason="Need --go-live option to run")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip)


def get_credentials() -> dict:
    """Returns the MySQL credentials for the localhost"""
    credentials = read_yaml(Path(__file__).parent/"credentials.yaml")
    return credentials


@pytest.fixture
def plastic_mock():

    def _plastic_mock(side_effects: list) -> Callable:
        """
        Dynamic mock closure which returns objects from a list of objects as
        mock side-effects.
        """
        gen = (x for x in side_effects)

        def mock_return(*args, **kwargs):
            return next(gen)

        return mock_return
    return _plastic_mock


@pytest.fixture(scope="function")
def cnx():
    """Returns a MySQL connection object for the localhost"""
    credentials = get_credentials()
    cnx = mysql.connector.connect(**credentials)
    yield cnx

    cnx.rollback() # Cleanup
    cnx.close()


@pytest.fixture(scope="function")
def client(cnx):
    """Returns an instance of the MySQLClient object"""
    client = MySQLClient(cnx)
    yield client

    # We don't want out test database to persist, so remove it when we're done
    # using the fixture.
    try:
        with closing(client.cursor()) as cursor:
            cursor.execute(f"DROP DATABASE {TEMP_DB}")
    except mysql.connector.Error as err:
        pass
