import os
import json
from datetime import datetime
import pandas as pd
from .table_utils import calculate_hash, generate_table_name

class ProvenanceTracker:
    """Tracks data provenance (lineage) for pandas DataFrames.
    
    Automatically logs all transformations applied to DataFrames, enabling
    data lineage tracking, reproducibility, and auditability of data workflows.
    Provenance logs are persisted to JSON for inspection and sharing.
    """
    
    def __init__(self, log_file="provenance/provenance_log.json"):
        """Initialize ProvenanceTracker.
        
        Args:
            log_file (str): Path to JSON file for storing provenance logs.
                           Default: 'provenance/provenance_log.json'
        """
        self.log_file_path = os.path.abspath(log_file)
        self.provenance_entries = []
        self.session_dataframes = {}  # Dictionary to track session dataframes
        self.why_provenance_store = {}
        self.initialize_log_storage()

    def _make_tuple_reference(self, table_hash, row_index):
        return {
            "table_hash": str(table_hash),
            "row_index": int(row_index),
        }

    def _register_source_provenance(self, dataframe, table_hash):
        row_provenance = {}
        for row_index in dataframe.index.tolist():
            row_provenance[str(row_index)] = {
                "witness_sets": [[self._make_tuple_reference(table_hash, row_index)]],
                "source": "read"
            }
        self.why_provenance_store[table_hash] = row_provenance

    def _inherit_identity_provenance(self, output_dataframe, output_hash, input_dataframe, input_hash):
        input_store = self.why_provenance_store.get(input_hash, {})
        output_store = {}

        for output_index in output_dataframe.index.tolist():
            key = str(output_index)
            inherited = input_store.get(key)

            if inherited:
                output_store[key] = inherited
            else:
                output_store[key] = {
                    "witness_sets": [[self._make_tuple_reference(input_hash, output_index)]],
                    "source": "identity"
                }

        self.why_provenance_store[output_hash] = output_store

    def initialize_log_storage(self):
        log_dir = os.path.dirname(self.log_file_path)
        os.makedirs(log_dir, exist_ok=True)
        try:
            with open(self.log_file_path, "r") as log_file:
                self.provenance_entries = json.load(log_file)
        except (FileNotFoundError, json.JSONDecodeError):
            self.provenance_entries = []
            with open(self.log_file_path, "w"):
                pass

    def persist_provenance_log(self):
        log_dir = os.path.dirname(self.log_file_path)
        os.makedirs(log_dir, exist_ok=True)
        with open(self.log_file_path, "w") as log_file:
            json.dump(self.provenance_entries, log_file, indent=4)

    def track_table_transformation(self, dataframe, source_file=None, transformation_type=None, transformation_details=None, input_dataframes=None):
        """Track and log a DataFrame transformation.
        
        Args:
            dataframe (pd.DataFrame): The resulting DataFrame after transformation.
            source_file (str, optional): Source file path (for read operations).
            transformation_type (str, optional): Type of transformation (filter, merge, etc.).
            transformation_details (str, optional): Details about the transformation.
            input_dataframes (list, optional): List of input DataFrames involved in transformation.
            
        Returns:
            tuple: (transformed_dataframe, table_name)
        """
        table_identifier = calculate_hash(dataframe)
        generated_table_name = generate_table_name(table_identifier)
        transformation_timestamp = datetime.now().isoformat()
        
        transformation_rationale = self.extract_transformation_rationale(
            dataframe, 
            input_dataframes, 
            transformation_type, 
            transformation_details
        )

        provenance_record = {
            "table_name": generated_table_name,
            "table_hash": table_identifier,
            "source_file": source_file,
            "transformation_type": transformation_type,
            "transformation_specifics": transformation_details,
            "dataframe_columns": list(dataframe.columns),
            "dataframe_dimensions": dataframe.shape,
            "recorded_at": transformation_timestamp,
            "transformation_rationale": transformation_rationale,
            "why_provenance_rows": len(self.why_provenance_store.get(table_identifier, {})),
        }
        
        self.provenance_entries.append(provenance_record)
        self.persist_provenance_log()
        return dataframe, generated_table_name

    def extract_transformation_rationale(self, output_dataframe, input_dataframes, transformation_type, transformation_details):
        transformation_context = {
            "type": transformation_type,
            "input_output_relationship": [],
        }

        if transformation_type in ["filter", "column_removal", "selection"]:
            transformation_context["type"] = "single_dataframe_operation"
            transformation_context["input_output_relationship"] = {
                "input_row_indices": list(range(len(input_dataframes[0]))) if input_dataframes else [],
                "output_row_indices": list(range(len(output_dataframe))),
            }
            transformation_context["applied_conditions"] = transformation_details

            if transformation_type == "filter":
                matching_row_indices = input_dataframes[0].query(transformation_details).index.tolist()
                transformation_context["rows_satisfying_condition"] = matching_row_indices

        elif transformation_type == "merge":
            transformation_context["type"] = "multi_dataframe_operation"
            transformation_context["input_output_relationship"] = {
                "first_input_dataframe_rows": len(input_dataframes[0]) if input_dataframes else 0,
                "second_input_dataframe_rows": len(input_dataframes[1]) if len(input_dataframes) > 1 else 0,
                "output_rows": len(output_dataframe),
                "input_dataframe_hashes": [
                    calculate_hash(input_dataframes[0]),
                    calculate_hash(input_dataframes[1])
                ]
            }
            merged_row_indices = {
                "first_dataframe_row_indices": input_dataframes[0].index.tolist(),
                "second_dataframe_row_indices": input_dataframes[1].index.tolist(),
            }
            transformation_context["merged_row_indices"] = merged_row_indices

            transformation_context["merged_columns"] = {
                "first_dataframe_columns": input_dataframes[0].columns.tolist(),
                "second_dataframe_columns": input_dataframes[1].columns.tolist(),
            }

        return transformation_context

    def read_csv(self, filepath):
        """Read a CSV file and track it as an initial data source.
        
        Args:
            filepath (str): Path to the CSV file to read.
            
        Returns:
            tuple: (dataframe, table_name)
        """
        dataframe = pd.read_csv(filepath)
        table_hash = calculate_hash(dataframe)
        self.session_dataframes[table_hash] = dataframe
        self._register_source_provenance(dataframe, table_hash)
        
        return self.track_table_transformation(
            dataframe, 
            source_file=filepath, 
            transformation_type="read_csv"
        )

    def filter(self, df, condition):
        """Filter a DataFrame using a pandas query condition.
        
        Args:
            df (pd.DataFrame): Input DataFrame to filter.
            condition (str): Pandas query condition (e.g., 'A > 2').
            
        Returns:
            tuple: (filtered_dataframe, table_name)
        """
        filtered_dataframe = df.query(condition).copy()
        filtered_dataframe = filtered_dataframe.reset_index(drop=True)
        filtered_table_hash = calculate_hash(filtered_dataframe)
        input_hash = calculate_hash(df)
        self.session_dataframes[filtered_table_hash] = filtered_dataframe
        self._inherit_identity_provenance(filtered_dataframe, filtered_table_hash, filtered_dataframe, input_hash)
        
        return self.track_table_transformation(
            filtered_dataframe, 
            transformation_type="filter", 
            transformation_details=condition, 
            input_dataframes=[df]
        )

    def drop_columns(self, df, columns_to_drop):
        """Drop columns from a DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            columns_to_drop (list): List of column names to drop.
            
        Returns:
            tuple: (dataframe_without_columns, table_name)
        """
        dataframe_without_columns = df.drop(columns=columns_to_drop)
        output_hash = calculate_hash(dataframe_without_columns)
        input_hash = calculate_hash(df)
        self._inherit_identity_provenance(dataframe_without_columns, output_hash, dataframe_without_columns, input_hash)
        return self.track_table_transformation(
            dataframe_without_columns, 
            transformation_type="drop_columns", 
            transformation_details=f"columns_to_drop: {columns_to_drop}", 
            input_dataframes=[df]
        )

    def merge(self, df1, df2, how="inner", on=None):
        """Merge two DataFrames.
        
        Args:
            df1 (pd.DataFrame): First input DataFrame.
            df2 (pd.DataFrame): Second input DataFrame.
            how (str): Type of merge ('inner', 'outer', 'left', 'right'). Default: 'inner'
            on (str or list, optional): Column(s) to join on.
            
        Returns:
            tuple: (merged_dataframe, table_name)
        """
        merged_dataframe = df1.merge(df2, how=how, on=on)
        merged_hash = calculate_hash(merged_dataframe)
        self.why_provenance_store[merged_hash] = {
            str(idx): {
                "witness_sets": [],
                "source": "merge_pending_feature"
            }
            for idx in merged_dataframe.index.tolist()
        }
        return self.track_table_transformation(
            merged_dataframe, 
            transformation_type="merge", 
            transformation_details=f"how: {how}, on: {on}", 
            input_dataframes=[df1, df2]
        )

    def get_table_why_provenance(self, dataframe_or_hash):
        """Get why-provenance mapping for a full table.

        Args:
            dataframe_or_hash (pd.DataFrame or str): DataFrame instance or table hash.

        Returns:
            dict: Mapping from output row index to witness sets.
        """
        if isinstance(dataframe_or_hash, pd.DataFrame):
            table_hash = calculate_hash(dataframe_or_hash)
        else:
            table_hash = dataframe_or_hash
        return self.why_provenance_store.get(table_hash, {})

    def get_row_why_provenance(self, dataframe_or_hash, row_index):
        """Get why-provenance witness sets for a specific output row.

        Args:
            dataframe_or_hash (pd.DataFrame or str): DataFrame instance or table hash.
            row_index (int): Output row index.

        Returns:
            dict: Provenance details for the row or empty dict.
        """
        table_map = self.get_table_why_provenance(dataframe_or_hash)
        return table_map.get(str(row_index), {})