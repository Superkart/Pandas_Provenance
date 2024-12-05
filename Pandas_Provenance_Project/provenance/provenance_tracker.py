import os
import json
from datetime import datetime
from .table_utils import calculate_hash, generate_table_name
import pandas as pd

class ProvenanceTracker:
    def __init__(self, log_file="provenance/provenance_log.json"):
        # Get the absolute path for the log file
        self.log_file = os.path.abspath(log_file)
        self.logs = []
        self.load_logs()

    def load_logs(self):
        """Load existing logs from the JSON file."""
        log_dir = os.path.dirname(self.log_file)
        
        # Ensure the directory exists, if not create it
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        try:
            with open(self.log_file, "r") as f:
                self.logs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logs = []
            # Create an empty log file if it doesn't exist
            with open(self.log_file, "w"):
                pass

    def save_logs(self):
        """Save logs to the JSON file."""
        log_dir = os.path.dirname(self.log_file)
        
        # Ensure the directory exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        with open(self.log_file, "w") as f:
            json.dump(self.logs, f, indent=4)

    def track_table(self, df, source_file=None, operation=None, conditions=None, input_tables=None):
        """Track a DataFrame operation."""
        table_hash = calculate_hash(df)
        table_name = generate_table_name(table_hash)
        timestamp = datetime.now().isoformat()
        
        # Generate Why Provenance
        why_provenance = self.generate_why_provenance(df, input_tables, operation, conditions)

        entry = {
            "table_name": table_name,
            "hash": table_hash,
            "source_file": source_file,
            "operation": operation,
            "conditions": conditions,
            "columns": list(df.columns),
            "shape": df.shape,
            "timestamp": timestamp,
            "why_provenance": why_provenance,
        }
        self.logs.append(entry)
        self.save_logs()
        return df, table_name

    def generate_why_provenance(self, output_df, input_tables, operation, conditions):
        """Generate the Why Provenance based on the operation type."""
        why_provenance = {
            "type": operation,
            "input_to_output_mapping": [],
        }

        if operation in ["filter", "drop_columns", "selection"]:
            why_provenance["type"] = "single_table"
            why_provenance["input_to_output_mapping"] = {
                "input_rows": list(range(len(input_tables[0]))) if input_tables else [],
                "output_rows": list(range(len(output_df))),
            }
        elif operation == "merge":
            why_provenance["type"] = "multi_table"
            why_provenance["input_to_output_mapping"] = {
                "input_table_1": len(input_tables[0]) if input_tables else 0,
                "input_table_2": len(input_tables[1]) if len(input_tables) > 1 else 0,
                "output_rows": len(output_df),
                "input_table_hashes": [
                    calculate_hash(input_tables[0]),  # Calculate hash for the first input table
                    calculate_hash(input_tables[1])   # Calculate hash for the second input table
                ]
            }
        return why_provenance


    def read_csv(self, filepath):
        """Read a CSV file and track its provenance."""
        df = pd.read_csv(filepath)
        return self.track_table(df, source_file=filepath, operation="read_csv")

    def filter(self, df, condition):
        """Track a filter operation."""
        df_filtered = df.query(condition)  # Apply filter
        input_table_hash = [calculate_hash(df)]  # Get hash of input table
        return self.track_table(df_filtered, operation="filter", conditions=condition, input_tables=[df])

    def drop_columns(self, df, columns_to_drop):
        """Track a column drop operation."""
        df_dropped = df.drop(columns=columns_to_drop)
        input_table_hash = [calculate_hash(df)]  # Get hash of input table
        return self.track_table(df_dropped, operation="drop_columns", conditions=f"columns_to_drop: {columns_to_drop}", input_tables=[df])

    def merge(self, df1, df2, how="inner", on=None):
        """Track a merge operation."""
        df_merged = df1.merge(df2, how=how, on=on)
        input_table_hashes = [calculate_hash(df1), calculate_hash(df2)]  # Add hashes of both tables
        return self.track_table(df_merged, operation="merge", conditions=f"how: {how}, on: {on}", input_tables=[df1, df2])
