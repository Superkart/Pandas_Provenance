import pandas as pd
import json
from .table_utils import calculate_hash, generate_table_name


class ProvenanceTracker:
    def __init__(self, log_file="./Pandas_Provenance_Project/provenance/provenance_log.json"):
        self.log_file = log_file
        self.logs = []
        self.load_logs()

    def load_logs(self):
        """Load existing logs from the JSON file."""
        try:
            with open(self.log_file, "r") as f:
                self.logs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logs = []
            with open(self.log_file, "w"):
                pass

    def save_logs(self):
        """Save logs to the JSON file."""
        with open(self.log_file, "w") as f:
            json.dump(self.logs, f, indent=4)

    def track_table(self, df, source_file=None, operation=None, conditions=None):
        """Track a DataFrame operation."""
        table_hash = calculate_hash(df)
        table_name = generate_table_name(table_hash)
        entry = {
            "table_name": table_name,
            "hash": table_hash,
            "source_file": source_file,
            "operation": operation,
            "conditions": conditions,
            "columns": list(df.columns),
            "shape": df.shape
        }
        self.logs.append(entry)
        self.save_logs()
        return df, table_name

    def read_csv(self, filepath):
        """Read a CSV file and track its provenance."""
        df = pd.read_csv(filepath)
        return self.track_table(df, source_file=filepath, operation="read_csv")

    def filter(self, df, condition):
        """Track a filter operation."""
        df_filtered = df.query(condition)  # Apply filter
        return self.track_table(df_filtered, operation="filter", conditions=condition)

    def drop_columns(self, df, columns_to_drop):
        """Track a column drop operation."""
        df_dropped = df.drop(columns=columns_to_drop)
        return self.track_table(df_dropped, operation="drop_columns", conditions=f"columns_to_drop: {columns_to_drop}")

    def merge(self, df1, df2, how="inner", on=None):
        """Track a merge operation."""
        df_merged = df1.merge(df2, how=how, on=on)
        return self.track_table(df_merged, operation="merge", conditions=f"how: {how}, on: {on}")
