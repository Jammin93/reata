"""
Convenience wrapper for the MySQL DBAPI. Adds an additional layer of
abstraction to the connection object in order to simplify common procedures
like creating new databases, fetching rows from tables, and other common
MySQL operations.
"""
from __future__ import annotations

__author__ = "Ben Ohling"
__copyright__ = f"Copyright (C) 2022, {__author__}"
__version__ = "1.0.0"
__all__ = ["MySQLClient"]

import sys

import mysql.connector
import numpy as np
import pandas as pd
import pdxtra as pdx

from contextlib import closing, contextmanager
from decimal import Decimal
from functools import wraps
from typing import Any, Callable, Optional, Sequence, Union

from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errorcode import ER_BAD_DB_ERROR

from reata import schema


def autocommit(method: Callable) -> Callable:
    """
    Automatically commit the transaction using the class's context manager
    """

    @wraps(method)
    def wrapper(
            self: MySQLClient,
            *args,
            autocommit: bool = True,
            **kwargs,
            ) -> Any:
        # If the user sets the autocommit argument to false then the
        # autocommit behavior is overriden.
        if autocommit:
            with self:
                return method(self, *args, **kwargs)
        else:
            return method(self, *args, **kwargs)

    return wrapper


class MySQLClient:
    """Client wrapper for ``MySQLConnection`` objects."""

    def __init__(self, cnx: MySQLConnection) -> None:
        self.cnx = cnx

    def __enter__(self) -> MySQLClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Rollback the transaction if an exception is raised, else commit the
        transaction.
        """
        if "pytest" in sys.modules:
            # Do nothing if we're running inside of pytest
            pass
        elif exc_type:
            self.cnx.rollback()
        else:
            self.cnx.commit()

    def cursor(self, *args, **kwargs) -> MySQLCursor:
        """
        Exposes the underlying cursor constructor for the ``MySQLConnection``
        object which returns the requested cursor class. The cursor can be any
        subclass of ``MySQLCursor``.
        """
        return self.cnx.cursor(*args, **kwargs)

    @property
    def database(self) -> str:
        """The name of the currently selected database."""
        return self.cnx.database

    def use(self, database: str, auto_create: bool = False) -> None:
        """
        Switch the current database. If ``auto_create`` is ``True``, then
        create the database if it does not yet exist.

        Parameters
        ----------
        database : `str`
            The name of the database to which we wish to switch.
        auto_create : `bool`
            If ``True`` then attempt to create the database if it does not
            already exist. If ``False`` raise an error when attempting to
            connect to a non-existant database.

        Raises
        ------
        ``ERROR_BAD_DB_ERROR``:
            When the database does not exist and ``auto_create`` is set to
            ``False``.
        """
        stmt = f"USE {database}"
        with closing(self.cursor()) as cursor:
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                if err.errno == ER_BAD_DB_ERROR and auto_create:
                    self.create_database(database)
                    cursor.execute(stmt)
                else:
                    raise err

    @contextmanager
    def switch_db(self, db_name: str) -> None:
        """
        Temporarily switch the currently selected database.

        Parameters
        ----------
        db_name : `str`
            The name of the database to be temporarily selected.
        """
        initial_db = self.database
        try:
            # While in context, switch to the specified database
            self.use(db_name)
            yield
        finally:
            # Switch back to the database which was selected before entering
            # the context manager.
            self.use(initial_db)

    def create_database(self, name: str) -> None:
        """
        Creates a database only if it does not already exist.

        Parameters
        ----------
        name : `str`
            The name of the database to create.

        .. note::
           The method submits a "CREATE DATABASE" statement to MySQL with an
           "IF NOT EXISTS" clause attached. Thus, if the database already
           exists, the entire statement is ignored and will not raise an error.
        """
        stmt = (
            f"CREATE DATABASE IF NOT EXISTS {name} "
            f"DEFAULT CHARACTER SET 'utf8'"
        )
        with closing(self.cursor()) as cursor:
            cursor.execute(stmt)

    def create_table(self, schema: schema.TableSchema) -> None:
        """
        Create the specified table in the currently selected database.

        Parameters
        ----------
        schema : ``TableSchema``
            The table schema defining the table which is to be created
        """
        pk_clause = ""
        if schema.primary_key:
            pk_clause = f", PRIMARY KEY{schema.primary_key}"

        stmt = (
            f"CREATE TABLE IF NOT EXISTS "
            f"`{schema.name}` ({schema.columns}{pk_clause})"
        )
        with closing(self.cursor()) as cursor:
            cursor.execute(stmt)

    @autocommit
    def database_exists(self, name: str) -> bool:
        """
        Returns a boolean representing the existence of the database.

        Parameters
        ----------
        name : `str`
            The name of the database for which to search.

        Returns
        -------
        A boolean indicating whether or not the database exists.
        """
        stmt = (
            f"SELECT schema_name "
            f"FROM information_schema.schemata "
            f"WHERE schema_name='{name}'"
        )
        # Since we aren't consuming the results from the cursor and we expect
        # the overall result to be reasonably small, we use a buffered cursor
        # which can be closed without first being cleared.
        with closing(self.cursor(buffered=True)) as cursor:
            cursor.execute(stmt)
            db_exists = bool(cursor.rowcount)

        return db_exists

    @autocommit
    def table_exists(self, table_name: str) -> bool:
        """
        Returns a boolean indicating the existence of the table in the
        specified database.

        Parameters
        ----------
        table_name : `str`
            The name of the table for which to search.

        Returns
        -------
        A boolean indicating the existence of the table in the currently
        selected database.
        """
        stmt = f"SHOW TABLES LIKE '{table_name}'"
        with closing(self.cursor(buffered=True)) as cursor:
            cursor.execute(stmt)
            # Since we can't have duplicate table names in a given database and
            # since we're matching against the full table name, we will only
            # ever receive one row at most. Converting the row count to a
            # boolean is, thus, all we need to determine the existence of the
            # table.
            table_exists = bool(cursor.rowcount)

        return table_exists

    @autocommit
    def add_index(
            self,
            table_name: str,
            index_name: str,
            column_name: str,
            ) -> None:
        """
        Ad-hoc method for adding an index to a table which has already been
        created.

        Parameters
        ----------
        table_name : `str`
            The table to which we wish to add the index.
        index_name : `str`
            The name of the index.
        column_name : `str`
            The name of the column to be indexed.
        """
        stmt = (
            f"ALTER TABLE `{table_name}` "
            f"ADD INDEX `{index_name}` (`{column_name}`)"
        )
        with closing(self.cursor()) as cursor:
            cursor.execute(stmt)

    @autocommit
    def index_exists(
            self,
            table_name: str,
            index_name: str,
            column_name: str,
            ) -> bool:
        """
        Checks to see if the index exists on the specified column of the
        specified table.

        Parameters
        ----------
        table_name : `str`
            The table to which we wish to add the index.
        index_name : `str`
            The name of the index.
        column_name : `str`
            The name of the column to be indexed.

        Returns
        -------
        A boolean representing the existence of the index.
        """
        stmt = (
            f"SHOW INDEX FROM `{table_name}` "
            f"WHERE column_name='{column_name}'"
        )
        with closing(self.cursor(buffered=True)) as cursor:
            cursor.execute(stmt)
            idx_exists = bool(cursor.rowcount)

        return idx_exists

    @property
    @autocommit
    def table_names(self) -> set:
        """
        Returns the set of all tables stored in the currently selected
        database.
        """
        stmt = "SHOW TABLES"
        with closing(self.cursor()) as cursor:
            cursor.execute(stmt)
            tables = {x[0] for x in iter(cursor.fetchone, None)}

        return tables

    @autocommit
    def column_count(
            self,
            table_name: str,
            include_virtual: bool = False,
            ) -> int:
        """
        Returns the total number of columns found in the specified table.
        Optionally allows for inclusion of computed columns in the count.

        Parameters
        ----------
        table_name : `str`
            The name of the table on which the column count is being computed.
        include_virtual : `bool`, default `False`
            Whether or not to include virtual columns in the count.

        Returns
        -------
        An integer representing the total number of columns in the table.
        """
        stmt = (
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema='{self.database}' "
            f"AND table_name='{table_name}' "
        )
        if not include_virtual:
            stmt += (
                "AND extra != 'stored generated' "
                "AND extra != 'virtual generated'"
            )

        with closing(self.cursor(buffered=True)) as cursor:
            cursor.execute(stmt)
            num_cols = cursor.fetchone()[0]

        return num_cols

    @autocommit
    def column_names(
            self,
            table_name: str,
            include_virtual: bool = False,
            include_auto: bool = False,
            exclude: Optional[Union[Sequence, set]] = None,
            ) -> tuple:
        """
        Returns a ``tuple`` of column names from the table. Optionally
        allows for inclusion of both computed and/or auto-generated columns.
        The user may also exclude any additional column names from the
        result set that they desire.

        Parameters
        ----------
        table_name : `str`
            The name of the table which is being queried.
        include_virtual : `bool`, default `False`
            Whether or not to include computed columns in the result.
        include_auto : `bool`, default `False`
            Whether or not to include auto-generated columns in the result.
        exclude : {`Sequence`, `set`, `None`}, default `None`
            An iterable sequence of column names to exclude from the result.

        Returns
        -------
        A ``tuple`` containing the column names found in the table.
        """
        exclude = exclude or []
        stmt = f"SHOW columns FROM `{table_name}` " # Note the trailing space
        if include_auto and not include_virtual:
            stmt += (
                "WHERE extra != 'stored generated' "
                "AND extra != 'virtual stored'"
            )
        elif include_virtual and not include_auto:
            stmt += (
                "WHERE extra != 'auto_increment' "
                "AND extra NOT LIKE '%default_generated%'"
            )
        elif not include_virtual and not include_auto:
            stmt += (
                "WHERE extra != 'stored generated' "
                "AND extra != 'virtual stored' "
                "AND extra != 'auto_increment' "
                "AND extra NOT LIKE '%default_generated%'"
            )

        with closing(self.cursor(buffered=True)) as cursor:
            cursor.execute(stmt)
            col_names = tuple(x[0] for x in cursor if x[0] not in exclude)

        return col_names

    @autocommit
    def fetch_rows(
            self,
            table_name: str,
            columns: Optional[Union[Sequence, str]] = "*",
            where: Optional[str] = None,
            order_by: Optional[str] = None,
            offset: Optional[int] = 0,
            limit: Optional[int] = sys.maxsize,
            decimal_to_float: bool = False,
            map_records: bool = False,
            ) -> list:
        """
        Returns a ``tuple`` of rows from the table. Provides the option to
        offset the selection window, filter the results, sort by column,
        and limit the total number of returned records.

        Parameters
        ----------
        table_name : `str`
            The name of the table from which to fetch rows
        columns : {`Sequence`, `str`}, default "*"
            Some iterable sequence containing the names of the columns from
            which to fetch the data.
        where : `str`
            An optional "WHERE" clause in valid MySQL syntax.
        order_by : {`str`, `None`}, default `None`
            An optional "ORDER BY" clause in valid MySQL syntax.
        offset : {`int`, `None`}, default `None`
            The number of rows by which to offset the query.
        limit : `int`, default `sys.maxsize`
            Optional limit for the number of rows to be returned.
        decimal_to_float : `bool`, default `False`
            Whether or not to convert values of type ``Decimal`` to ``float``.
        map_records : `bool`, default `False`
            Whether or not to map the returned records. If ``True`` the rows
            returned will each be a dictionary with values mapped to column
            names.

        Returns
        -------
        A ``list`` of rows from the table.
        """
        # We take advantage of the fact that a join on a string of length one
        # always returns the unmodified string. Note the trailing space here.
        stmt = f"SELECT {', '.join(columns)} FROM `{table_name}` "
        if where:
            stmt += f"WHERE {where}"

        if order_by:
            stmt += f"ORDER BY {order_by} "

        # The default offset and limit arguments are effectively treated as
        # no-ops by MySQL.
        stmt += f"LIMIT {offset}, {limit}"
        with closing(self.cursor(dictionary=map_records)) as cursor:
            cursor.execute(stmt)
            if columns == "*" or len(columns) > 1:
                records = [x for x in iter(cursor.fetchone, None)]
            else:
                # When selecting more than one column, the return data becomes
                # nested, so we index each item over which we iterate.
                records = [x[0] for x in iter(cursor.fetchone, None)]

            if decimal_to_float and map_records:
                records = [
                    {k: float(v) if isinstance(v, Decimal) else v
                     for k, v in x.items()} for x in records
                ]
            elif decimal_to_float and not map_records:
                records = [
                    [float(v) if isinstance(v, Decimal) else v
                     for v in x] for x in records
                ]

        return records

    def fetch_table(
            self,
            table_name: str,
            columns: Optional[Union[Sequence, str]] = "*",
            where: Optional[str] = None,
            order_by: Optional[dict] = None,
            offset: Optional[int] = 0,
            limit: Optional[int] = sys.maxsize,
            decimal_to_float: bool = False,
            auto_downcast: bool = False,
            ) -> pdx.DataFrame:
        """
        Returns records from the table as a dataframe. Uses roughly the same
        arguments as ``fetch_rows`` with the added option to automatically
        downcast all numeric types in the returned dataframe.

        Parameters
        ----------
        table_name : `str`
            The name of the table from which to fetch rows
        columns : {`Sequence`, `str`}, default "*"
            Some iterable sequence containing the names of the columns from
            which to fetch the data.
        where : `str`
            An optional "WHERE" clause in valid MySQL syntax.
        order_by : {`str`, `None`}, default `None`
            An optional "ORDER BY" clause in valid MySQL syntax.
        offset : {`int`, `None`}, default `None`
            The number of rows by which to offset the query.
        limit : `int`, default `sys.maxsize`
            Optional limit for the number of rows to be returned.
        decimal_to_float : `bool`, default `False`
            Whether or not to convert values of type ``Decimal`` to ``float``.
        auto_downcast : `bool`
            Whether or not to downcast numeric types in the returned dataframe.

        Returns
        -------
        A PDXtra ``DataFrame`` object.
        """
        records = self.fetch_rows(
            table_name=table_name,
            columns=columns,
            where=where,
            order_by=order_by,
            offset=offset,
            limit=limit,
            decimal_to_float=decimal_to_float,
            map_records=True
        )
        df = pdx.DataFrame(records).replace({None: np.nan})
        if auto_downcast:
            df = pdx.downcast_dataframe(df)

        df = df.convert_dtypes(
            infer_objects=True,
            convert_string=False,
            convert_boolean=False,
            convert_integer=True,
            convert_floating=True,
        )
        return df

    @autocommit
    def bulk_insert(
            self,
            table_name: str,
            columns: Sequence[str],
            records: Sequence[Sequence],
            update_method: Optional[str] = None, # upsert/replace
            ) -> None:
        """
        Performs an insert for each of the records in a sequence of records.
        Can optionally update records for duplicate key values by either
        "upserting" the record or replacing the record entirely.

        Parameters
        ----------
        table_name : `str`
            Name of the table into which the records are being inserted.
        columns : `Sequence`
            List of column names into which the data is being inserted.
        records : `Sequence`
            The records to be inserted into the table.
        update_method : {`str`, `None`}, default `None`
            Whether or not to update records with duplicate keys.

            - ``None`` (default): Do not update records with duplicate keys.
            - upsert: Add an "ON DUPLICATE KEY UPDATE" clause to the
              "INSERT" statement to update records.
            - replace: Use a "REPLACE" statement in place of the "INSERT"
              statement.
        """
        cols = self._escaped(columns)
        proxies = self._get_proxies(len(columns))
        if update_method is None:
            stmt = f"INSERT INTO `{table_name}` {cols} VALUES {proxies}"
        elif update_method == "upsert":
            # If updating duplicate key values, add an update clause to the
            # statement, forcing MySQL to append a new row. The old row, which
            # contains the old duplicate key value, is then invalidated after
            # the transaction is committed.
            stmt = (
                f"INSERT INTO `{table_name}` {cols} VALUES {proxies} "
                f"AS vals ON DUPLICATE KEY UPDATE "
                f"{', '.join((f'{x}=vals.{x}' for x in columns))}"
            )
        elif update_method == "replace":
            stmt = f"REPLACE INTO `{table_name}` {cols} VALUES {proxies}"
        elif not isinstance(update_method, str):
            raise TypeError(
                f"expected 'update_method' to be of type 'str'; "
                f"got type '{type(update_method)}' instead."
            )
        else:
            raise ValueError(
                f"expected 'update_method' to be one of: 'upsert', 'replace'; "
                f"got '{update_method}' instead."
            )

        with closing(self.cursor()) as cursor:
            cursor.executemany(stmt, records)

    def dataframe_insert(
            self,
            table_name: str,
            df: pd.DataFrame,
            include_index: bool = True,
            update_method: Optional[str] = None,
            ) -> None:
        """
        Insert data into a table directly from a Pandas dataframe.

        Parameters
        ----------
        table_name : `str`
            Name of the table into which the records are being inserted.
        df : `DataFrame`
            A Pandas dataframe containing the records to be inserted.
        include_index : `bool`
            Whether or not to include the dataframe index in the records.
        update_method : {`str`, `None`}, default `None`
            Whether or not to update records with duplicate keys.

            - ``None`` (default): Do not update records with duplicate keys.
            - upsert: Add an "ON DUPLICATE KEY UPDATE" clause to the
              "INSERT" statement to update records.
            - replace: Use a "REPLACE" statement in place of the "INSERT"
              statement.
        """
        columns = [x for x in df.columns]
        if include_index:
            records = df.to_records()
        else:
            records = df.to_numpy().tolist()

        self.bulk_insert(table_name, columns, records, update_method)

    def _get_proxies(self, column_count: int) -> str:
        """
        Generate a string of variable name placeholders for the SQL statement
        """
        proxies = tuple(["%s"] * column_count)
        proxies = f"({', '.join(proxies)})"
        return proxies

    def _escaped(self, columns: tuple) -> str:
        """Escape the columns and format for the SQL statement"""
        return f"({', '.join((f'`{x}`' for x in columns))})"

    def close(self):
        """Close the MySQL connection."""
        self.cnx.close()
