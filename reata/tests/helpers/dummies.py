import re

from datetime import date
from pathlib import Path

from hypothesis import strategies as st
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.numpy import arrays

from reata.schema import TableSchema
from reata.tests.conftest import read_yaml

samples = read_yaml(Path(__file__).parent/"strategies.yaml")

# Strategies for testing the MySQL DBAPI
table_names = samples["table_names"]
sql_types = samples["sql_types"]
sql_dicts = st.fixed_dictionaries({
    "id": st.sampled_from(sql_types["integer"]),
    "name": st.sampled_from(sql_types["varchar"]),
    "values": st.sampled_from(sql_types["decimal"])
})
sql_tables = st.builds(
    TableSchema,
    name=st.sampled_from(table_names),
    columns=sql_dicts,
    primary_key=st.just(("id", ))
)
