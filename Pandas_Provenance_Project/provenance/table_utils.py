import hashlib

def calculate_hash(df):
    """Calculate a hash for a DataFrame's content."""
    data_string = df.to_csv(index=False).encode()
    return hashlib.sha256(data_string).hexdigest()

def generate_table_name(table_hash):
    """Generate a deterministic table name based on its hash."""
    return f"table_{table_hash[:8]}"
