import re

import hypothesis.strategies as st
import pytest

from hypothesis import given, HealthCheck, settings

from reata.tests.helpers import dummies
from reata import schema


class TestTable:

    @given(st.sampled_from(dummies.table_names), dummies.sql_dicts)
    def test_table(self, table_name, table_schema):
        pk = ("id", )
        table = schema.TableSchema(table_name, table_schema, primary_key=pk)
        assert table.name == table_name
        assert table.columns == table_schema
        assert table.primary_key.key_columns == pk
