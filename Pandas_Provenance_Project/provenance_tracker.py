import json
from .table_utils import hash_table, generate_table_name

class ProvenanceTracker:
    def __init__(self, log_file="provenance/provenance_log.json"):
        self.log_file = log_file
        self.provenance_log = []
        self.load_log()

    def load_log(self):
        """Load the provenance log from the JSON file."""
        try:
            with open(self.log_file, 'r') as file:
                self.provenance_log = json.load(file)
        except FileNotFoundError:
            self.provenance_log = []

    def save_log(self):
        """Save the provenance log to the JSON file."""
        with open(self.log_file, 'w') as file:
            json.dump(self.provenance_log, file, indent=4)

    def track_table(self, dataframe, table_name, derived_from=None, operation=None):
        """Track a DataFrame in the provenance log."""
        table_hash = hash_table(dataframe)
        log_entry = {
            'table_name': table_name,
            'table_hash': table_hash,
            'derived_from': derived_from or [],
            'operation': operation or 'load'
        }
        self.provenance_log.append(log_entry)
        self.save_log()

    def add_why_provenance(self, dataframe, parent_rows=None):
        """Add a why_provenance column to a DataFrame."""
        if parent_rows:
            dataframe['why_provenance'] = dataframe.apply(
                lambda row: frozenset(parent_rows), axis=1
            )
        else:
            dataframe['why_provenance'] = dataframe.apply(
                lambda row: frozenset({tuple(row)}), axis=1
            )
        return dataframe
