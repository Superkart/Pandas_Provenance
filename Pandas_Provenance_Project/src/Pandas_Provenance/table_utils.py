import hashlib
import pandas as pd

def calculate_hash(dataframe):
    if "why_provenance" in dataframe.columns:
        dataframe = dataframe.drop(columns=["why_provenance"])

    hash_object = hashlib.sha256(pd.util.hash_pandas_object(dataframe).values)
    return hash_object.hexdigest()

def generate_table_name(table_hash):
    return f"table_{table_hash[:8]}"