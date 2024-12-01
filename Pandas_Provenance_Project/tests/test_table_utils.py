import pandas as pd
from provenance.table_utils import hash_table, generate_table_name


def test_hash_table():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    hash1 = hash_table(df)
    hash2 = hash_table(df)
    assert hash1 == hash2

def test_generate_table_name():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    table_name = generate_table_name(df)
    assert table_name.startswith("table_")
