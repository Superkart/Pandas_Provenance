import pandas as pd
import json
from .table_utils import calculate_hash, generate_table_name

class ProvenanceTracker:
    def __init__(self, log_file="provenance_log.json"):
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
