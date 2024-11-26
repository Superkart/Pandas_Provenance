import hashlib

def hash_table(dataframe):
    """Generate a hash for the content and schema of a DataFrame."""
    content_string = dataframe.to_csv(index=False)
    return hashlib.md5(content_string.encode()).hexdigest()

def generate_table_name(input_tables, operation):
    """Generate a deterministic name for a derived table."""
    name = f"{operation}_" + "_".join(input_tables)
    return hashlib.md5(name.encode()).hexdigest()
