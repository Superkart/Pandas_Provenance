import pytest
import pandas as pd
from provenance.provenance_tracker import ProvenanceTracker



def test_read_csv(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)
    
    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = f.read()
    assert "read_csv" in logs
    assert table_name in logs


def test_filter_operation(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)
    filtered_df, filtered_table_name = tracker.filter(df, "Name=='Red'")
    
    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = f.read()
    assert "filter" in logs
    assert filtered_table_name in logs


def test_drop_columns_operation(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)
    dropped_df, dropped_table_name = tracker.drop_columns(df, ["HEX"])
    
    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = f.read()
    assert "drop_columns" in logs
    assert dropped_table_name in logs

