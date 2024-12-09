import os
import json
from datetime import datetime
from .table_utils import calculate_hash, generate_table_name
import pandas as pd

class ProvenanceTracker:

    def __init__(self, log_File = "provenance/provenance_log.json"):
        self.log_File_Path = os.path.abspath(log_File)
        self.provenance_Entries = []
        self.Initialize_Log_Storage()

    def Initialize_Log_Storage(self):
        log_Dir = os.path.dirname(self.log_File_Path)
        
        os.makedirs(log_Dir, exist_ok = True)
        
        try:
            with open(self.log_File_Path, "r") as log_File:
                self.provenance_Entries = json.load(log_File)
        except (FileNotFoundError, json.JSONDecodeError):
            self.provenance_Entries = []
            with open(self.log_File_Path, "w"):
                pass

    def Persist_Provenance_Log(self):
        log_Dir = os.path.dirname(self.log_File_Path)
        
        os.makedirs(log_Dir, exist_ok=True)
        
        with open(self.log_File_Path, "w") as log_file:
            json.dump(self.provenance_Entries, log_file, indent=4)

    def Track_Table_Transformation(self, dataframe, source_file = None, transformation_Type = None, transformation_Details = None, input_Dataframes = None):
        table_Identifier = calculate_hash(dataframe)
        generated_Table_Name = generate_table_name(table_Identifier)
        transformation_Timestamp = datetime.now().isoformat()
        
        transformation_Rationale = self.Extract_Transformation_Rationale(
            dataframe, 
            input_Dataframes, 
            transformation_Type, 
            transformation_Details
        )

        provenance_Record = {
            "table_name": generated_Table_Name,
            "table_hash": table_Identifier,
            "source_file": source_file,
            "transformation_type": transformation_Type,
            "transformation_specifics": transformation_Details,
            "dataframe_columns": list(dataframe.columns),
            "dataframe_dimensions": dataframe.shape,
            "recorded_at": transformation_Timestamp,
            "transformation_rationale": transformation_Rationale,
        }
        
        self.provenance_Entries.append(provenance_Record)
        self.Persist_Provenance_Log()
        return dataframe, generated_Table_Name

    def Extract_Transformation_Rationale(self, output_Dataframe, input_Dataframes, transformation_Type, transformation_Details):
        transformation_Context = {
            "type": transformation_Type,
            "input_output_relationship": [],
        }

        if transformation_Type in ["filter", "column_removal", "selection"]:
            transformation_Context["type"] = "single_dataframe_operation"
            transformation_Context["input_output_relationship"] = {
                "input_row_indices": list(range(len(input_Dataframes[0]))) if input_Dataframes else [],
                "output_row_indices": list(range(len(output_Dataframe))),
            }
            transformation_Context["applied_conditions"] = transformation_Details

            if transformation_Type == "filter":
                matching_row_indices = input_Dataframes[0].query(transformation_Details).index.tolist()
                transformation_Context["rows_satisfying_condition"] = matching_row_indices

        elif transformation_Type == "merge":
            transformation_Context["type"] = "multi_dataframe_operation"
            transformation_Context["input_output_relationship"] = {
                "first_input_dataframe_rows": len(input_Dataframes[0]) if input_Dataframes else 0,
                "second_input_dataframe_rows": len(input_Dataframes[1]) if len(input_Dataframes) > 1 else 0,
                "output_rows": len(output_Dataframe),
                "input_dataframe_hashes": [
                    calculate_hash(input_Dataframes[0]),
                    calculate_hash(input_Dataframes[1])
                ]
            }
            merged_row_indices = {
                "first_dataframe_row_indices": input_Dataframes[0].index.tolist(),
                "second_dataframe_row_indices": input_Dataframes[1].index.tolist(),
            }
            transformation_Context["merged_row_indices"] = merged_row_indices

            transformation_Context["merged_columns"] = {
                "first_dataframe_columns": input_Dataframes[0].columns.tolist(),
                "second_dataframe_columns": input_Dataframes[1].columns.tolist(),
            }

        return transformation_Context

    def read_csv(self, filepath):
        dataframe = pd.read_csv(filepath)
        return self.Track_Table_Transformation(dataframe, source_file=filepath, transformation_Type="read_csv")

    def filter(self, df, condition):
        filtered_dataframe = df.query(condition)
        return self.Track_Table_Transformation(
            filtered_dataframe, 
            transformation_Type="filter", 
            transformation_Details=condition, 
            input_Dataframes=[df]
        )

    def drop_columns(self, df, columns_to_drop):
        dataframe_without_columns = df.drop(columns=columns_to_drop)
        return self.Track_Table_Transformation(
            dataframe_without_columns, 
            transformation_Type="drop_columns", 
            transformation_Details=f"columns_to_drop: {columns_to_drop}", 
            input_Dataframes=[df]
        )

    def merge(self, df1, df2, how="inner", on=None):
        merged_dataframe = df1.merge(df2, how=how, on=on)
        return self.Track_Table_Transformation(
            merged_dataframe, 
            transformation_Type="merge", 
            transformation_Details=f"how: {how}, on: {on}", 
            input_Dataframes=[df1, df2]
        )