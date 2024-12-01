import hashlib
import pandas as pd


def calculate_hash(df):
    """Calculate a unique hash for a DataFrame based on its contents."""
    hash_object = hashlib.sha256(pd.util.hash_pandas_object(df).values)
    return hash_object.hexdigest()


def generate_table_name(table_hash):
    """Generate a table name based on its hash."""
    return f"table_{table_hash[:8]}"
