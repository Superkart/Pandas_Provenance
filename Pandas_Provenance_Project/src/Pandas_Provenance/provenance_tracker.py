import os
import json
from datetime import datetime
import pandas as pd
from .table_utils import calculate_hash, generate_table_name

class ProvenanceTracker:
    def __init__(self, log_file="provenance/provenance_log.json"):
        self.log_file_path = os.path.abspath(log_file)
        self.provenance_entries = []
        self.session_dataframes = {}  # A dictionary to track session dataframes
        self.initialize_log_storage()

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
        dataframe = pd.read_csv(filepath)
        table_hash = calculate_hash(dataframe)
        if "why_provenance" not in dataframe.columns:
            dataframe["why_provenance"] = dataframe.index.map(lambda idx: {frozenset({(table_hash, idx)})})
        
        else:
            dataframe["why_provenance"] = dataframe["why_provenance"].apply(lambda prov: prov | {frozenset({(table_hash, idx)}) for idx in dataframe.index})

        self.session_dataframes[table_hash] = dataframe
        return self.Track_Table_Transformation(dataframe, source_file=filepath, transformation_Type="read_csv")

    def filter(self, df, condition):
        
        if "why_provenance" not in df.columns:
            table_hash = calculate_hash(df)
            df["why_provenance"] = df.index.map(lambda idx: {frozenset({(table_hash, idx)})})

        filtered_dataframe = df.query(condition).copy()
        filtered_dataframe["why_provenance"] = filtered_dataframe.index.map(lambda idx: df.loc[idx, "why_provenance"] if idx in df.index else set())

        filtered_table_hash = calculate_hash(filtered_dataframe)
        self.session_dataframes[filtered_table_hash] = filtered_dataframe
        
        return self.track_table_transformation(
            filtered_dataframe, 
            transformation_type="filter", 
            transformation_details=condition, 
            input_dataframes=[df]
        )

    def drop_columns(self, df, columns_to_drop):
        dataframe_without_columns = df.drop(columns=columns_to_drop)
        return self.track_table_transformation(
            dataframe_without_columns, 
            transformation_type="drop_columns", 
            transformation_details=f"columns_to_drop: {columns_to_drop}", 
            input_dataframes=[df]
        )

    def merge(self, df1, df2, how="inner", on=None):
        merged_dataframe = df1.merge(df2, how=how, on=on)
        return self.track_table_transformation(
            merged_dataframe, 
            transformation_type="merge", 
            transformation_details=f"how: {how}, on: {on}", 
            input_dataframes=[df1, df2]
        )