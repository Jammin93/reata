"""
Module for table schema and column data type objects which are used to define
table schemas for MySQL.
"""
from __future__ import annotations

__author__ = "Ben Ohling"
__copyright__ = f"Copyright (C) 2022, {__author__}"
__version__ = "1.0.0"
__all__ = ["TableSchema"]

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TableSchema:
    """
    MySQL table schema object for defining MySQL tables.

    Parameters
    ----------
    name : `str`
        The name of the MySQL table.
    columns : `dict`
        A dictionary containing column names as keys and their respective
        types as values. See example below.
    primary_key : `tuple`
        Tuple containing the names of the columns to be used as a primary key.

    Returns
    -------
    A ``TableSchema`` object.

    Examples
    ^^^^^^^^
    Create a table schema for company employees containing columns for
    record creation time, employee name, and employee age, then pretty-print
    the schema.

    .. note::
       The columns are being passed as a positional argument here.

    >>> from reata.schema import TableSchema
    >>> schema = TableSchema(
    ... "Employees", {
    ...         "ctime": "TIMESTAMP NOT NULL",
    ...         "name": "VARCHAR(32) NOT NULL",
    ...         "age": "TINYINT(3) DEFAULT NULL",
    ...     },
    ...     primary_key=("ctime", "name"),
    ... )
    >>> print(schema)
    Employees
    ctime    TIMESTAMP NOT NULL
    name     VARCHAR(32) NOT NULL
    age      TINYINT(3) DEFAULT NULL
    """

    class Columns(dict):

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)

        def __str__(self) -> str:
            return ", ".join((f"`{k}` {v}" for k, v in self.items()))

    @dataclass(slots=True)
    class PrimaryKey:
        key_columns: tuple[str]

        def __str__(self) -> str:
            key = f"({', '.join((f'`{x}`' for x in self.key_columns))})"
            return key

        def __getitem__(self, key: Any) -> str:
            return self.key_columns[key]

        def __len__(self) -> int:
            return len(self.key_columns)

        def __iter__(self) -> str:
            yield from self.key_columns

    name: str
    columns: dict = field(default_factory=dict)
    primary_key: tuple = field(default_factory=tuple)

    def __post_init__(self) -> None:
        self.columns = self.Columns(self.columns)
        self.primary_key = self.PrimaryKey(self.primary_key)

    def __str__(self) -> str:
        # Convoluted means of computing the spacing between a key and its
        # value, in order to generate a string where each new line contains a
        # key-value pair aligned to a vertical margin.
        max_key_len = len(max(self.columns.keys(), key=len))
        return f"{self.name}\n" + "\n".join([
            k + " " * (4 + max_key_len - len(k)) + str(v)
            for k, v in self.columns.items()
        ])
