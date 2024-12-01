import json
from .table_utils import hash_table, generate_table_name


class ProvenanceTracker:
    def __init__(self, log_file="provenance_log.json"):
        self.log_file = log_file
        self.provenance_log = self._load_log()

    def _load_log(self):
        """Loads the existing provenance log if it exists, otherwise initializes an empty log."""
        try:
            with open(self.log_file, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            return {}

    def _save_log(self):
        """Saves the current provenance log to a file."""
        with open(self.log_file, "w") as file:
            json.dump(self.provenance_log, file, indent=4)

    def track_table(self, dataframe, source_tables=None):
        """Tracks a DataFrame, its source, and its metadata."""
        table_name = generate_table_name(dataframe)
        table_hash = hash_table(dataframe)

        # Log the table provenance
        self.provenance_log[table_name] = {
            "source_tables": source_tables or [],
            "columns": list(dataframe.columns),
            "shape": dataframe.shape,
            "hash": table_hash,
        }

        self._save_log()
        return table_name

    def get_provenance(self, table_name):
        """Returns the provenance for a given table."""
        return self.provenance_log.get(table_name, "No provenance found.")

    def add_why_provenance(self, dataframe, why_column_name="why"):
        """Adds tuple-level why provenance to a DataFrame."""
        dataframe[why_column_name] = dataframe.index.to_series().apply(
            lambda idx: {f"Tuple-{idx}"}
        )
        return dataframe
