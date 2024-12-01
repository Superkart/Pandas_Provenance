import hashlib


def hash_table(dataframe):
    """Generates a hash for a DataFrame's content and columns."""
    table_bytes = dataframe.to_csv(index=False).encode()
    return hashlib.md5(table_bytes).hexdigest()


def generate_table_name(dataframe):
    """Creates a deterministic name for a DataFrame based on its hash."""
    return f"table_{hash_table(dataframe)}"
