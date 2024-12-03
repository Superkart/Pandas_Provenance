import pytest
import pandas as pd
import json
from provenance.provenance_tracker import ProvenanceTracker


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    data = """Name,HEX,RGB
    Red,#FF0000,255,0,0
    Green,#00FF00,0,255,0
    Blue,#0000FF,0,0,255"""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(data)
    return str(csv_file)


def test_read_csv(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)

    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = json.load(f)

    assert any(log["operation"] == "read_csv" for log in logs)
    assert any(log["table_name"] == table_name for log in logs)
    assert any("timestamp" in log for log in logs)


def test_filter_operation(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)
    filtered_df, filtered_table_name = tracker.filter(df, "Name=='Red'")

    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = json.load(f)

    filter_log = next(log for log in logs if log["table_name"] == filtered_table_name)
    assert filter_log["operation"] == "filter"
    assert filter_log["conditions"] == "Name=='Red'"
    assert "timestamp" in filter_log
    assert filter_log["why_provenance"]["type"] == "single_table"


def test_drop_columns_operation(sample_csv):
    tracker = ProvenanceTracker()
    df, table_name = tracker.read_csv(sample_csv)
    dropped_df, dropped_table_name = tracker.drop_columns(df, ["HEX"])

    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = json.load(f)

    drop_log = next(log for log in logs if log["table_name"] == dropped_table_name)
    assert drop_log["operation"] == "drop_columns"
    assert drop_log["conditions"] == "columns_to_drop: ['HEX']"
    assert "timestamp" in drop_log
    assert drop_log["why_provenance"]["type"] == "single_table"


def test_merge_operation(sample_csv):
    tracker = ProvenanceTracker()
    df1, table_name1 = tracker.read_csv(sample_csv)
    df2, table_name2 = tracker.read_csv(sample_csv)
    merged_df, merged_table_name = tracker.merge(df1, df2, on="Name")

    # Check if provenance logs have been updated
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = json.load(f)

    merge_log = next(log for log in logs if log["table_name"] == merged_table_name)
    assert merge_log["operation"] == "merge"
    assert "timestamp" in merge_log
    assert merge_log["why_provenance"]["type"] == "multi_table"
    assert "input_to_output_mapping" in merge_log["why_provenance"]
